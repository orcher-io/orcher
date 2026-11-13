//! Workflow configuration (coordination, execution, state, scheduling, retry, limits).

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Duration;

/// Workflow engine configuration
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct WorkflowConfig {
    /// Workflow coordination mode configuration
    pub coordination: WorkflowCoordinationConfig,

    /// Workflow execution configuration
    pub execution: WorkflowExecutionConfig,

    /// Workflow state persistence configuration
    pub state: WorkflowStateConfig,

    /// Workflow scheduling configuration
    pub scheduling: WorkflowSchedulingConfig,

    /// Workflow retry configuration
    pub retry: WorkflowRetryConfig,

    /// Workflow limits and quotas
    pub limits: WorkflowLimitsConfig,
}

/// Workflow coordination mode
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "lowercase")]
pub enum CoordinationMode {
    /// Local (in-process) coordination
    #[default]
    Local,
    /// Distributed (queue-based) coordination
    Distributed,
}

/// Workflow coordination configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowCoordinationConfig {
    /// Coordination mode
    pub mode: CoordinationMode,

    /// Local mode configuration
    pub local: LocalModeConfig,

    /// Distributed mode configuration
    pub distributed: DistributedModeConfig,
}

/// Local mode configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LocalModeConfig {
    /// Maximum concurrent workflows
    pub max_concurrent_workflows: u32,

    /// Persist workflow state to storage
    pub persist_state: bool,

    /// Workflow timeout in seconds
    pub workflow_timeout_secs: u64,
}

/// Distributed mode configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DistributedModeConfig {
    /// Workflow message stream name
    pub workflow_stream: String,

    /// Task message stream name
    pub task_stream: String,

    /// Consumer group name
    pub consumer_group: String,

    /// Batch size for message processing
    pub batch_size: usize,

    /// Enable partitioning for horizontal scaling
    pub enable_partitioning: bool,

    /// Number of partitions
    pub partition_count: u32,
}

/// Workflow execution configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowExecutionConfig {
    /// Maximum concurrent workflows
    pub max_concurrent_workflows: u32,

    /// Default workflow timeout
    pub default_timeout: Duration,

    /// Maximum workflow timeout
    pub max_timeout: Duration,

    /// Execution mode
    pub execution_mode: WorkflowExecutionMode,

    /// Step execution configuration
    pub step_execution: StepExecutionConfig,

    /// Workflow variables configuration
    pub variables: WorkflowVariablesConfig,
}

/// Workflow execution modes
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum WorkflowExecutionMode {
    /// Sequential execution (steps run one after another)
    Sequential,
    /// Parallel execution (independent steps run concurrently)
    Parallel,
    /// Mixed execution (combination based on dependencies)
    Mixed,
}

/// Step execution configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StepExecutionConfig {
    /// Maximum concurrent steps per workflow
    pub max_concurrent_steps: u32,

    /// Default step timeout
    pub default_step_timeout: Duration,

    /// Step retry configuration
    pub retry: StepRetryConfig,

    /// Step output size limits
    pub output_size_limit: u64,
}

/// Step retry configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StepRetryConfig {
    /// Maximum retry attempts per step
    pub max_attempts: u32,

    /// Initial retry delay
    pub initial_delay: Duration,

    /// Maximum retry delay
    pub max_delay: Duration,

    /// Retry backoff multiplier
    pub backoff_multiplier: f64,

    /// Retry on specific conditions
    pub retry_conditions: Vec<RetryCondition>,
}

/// Conditions that trigger step retries
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum RetryCondition {
    /// Retry on any error
    AnyError,
    /// Retry on specific error codes
    ErrorCodes(Vec<String>),
    /// Retry on timeout
    Timeout,
    /// Retry on resource unavailability
    ResourceUnavailable,
    /// Retry on network errors
    NetworkError,
}

/// Workflow variables configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowVariablesConfig {
    /// Maximum number of variables per workflow
    pub max_variables: u32,

    /// Maximum variable size in bytes
    pub max_variable_size: u64,

    /// Variable encryption settings
    pub encryption: VariableEncryptionConfig,

    /// Variable validation settings
    pub validation: VariableValidationConfig,
}

/// Variable encryption configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VariableEncryptionConfig {
    /// Enable variable encryption
    pub enabled: bool,

    /// Encryption algorithm
    pub algorithm: String,

    /// Patterns for variables that should be encrypted
    pub encrypt_patterns: Vec<String>,
}

/// Variable validation configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VariableValidationConfig {
    /// Enable variable validation
    pub enabled: bool,

    /// Variable type validation rules
    pub type_validation: HashMap<String, String>,

    /// Variable constraint validation
    pub constraint_validation: HashMap<String, String>,
}

/// Workflow state persistence configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowStateConfig {
    /// State persistence mode
    pub persistence_mode: StatePersistenceMode,

    /// State checkpoint configuration
    pub checkpoints: StateCheckpointConfig,

    /// State cleanup configuration
    pub cleanup: StateCleanupConfig,

    /// State compression settings
    pub compression: StateCompressionConfig,
}

/// State persistence modes
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum StatePersistenceMode {
    /// In-memory only (no persistence)
    Memory,
    /// Database persistence
    Database,
    /// File system persistence
    FileSystem,
    /// Hybrid (memory + periodic persistence)
    Hybrid,
}

/// State checkpoint configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StateCheckpointConfig {
    /// Enable automatic checkpoints
    pub enabled: bool,

    /// Checkpoint interval
    pub interval: Duration,

    /// Checkpoint on step completion
    pub on_step_completion: bool,

    /// Maximum checkpoint history to keep
    pub max_history: u32,
}

/// State cleanup configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StateCleanupConfig {
    /// Enable automatic cleanup
    pub enabled: bool,

    /// Cleanup completed workflows after duration
    pub completed_after: Duration,

    /// Cleanup failed workflows after duration
    pub failed_after: Duration,

    /// Cleanup cancelled workflows after duration
    pub cancelled_after: Duration,

    /// Cleanup orphaned state after duration
    pub orphaned_after: Duration,
}

/// State compression configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StateCompressionConfig {
    /// Enable state compression
    pub enabled: bool,

    /// Compression algorithm
    pub algorithm: String,

    /// Compression level (1-9)
    pub level: u8,

    /// Minimum state size to compress
    pub min_size: u64,
}

/// Workflow scheduling configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowSchedulingConfig {
    /// Scheduling strategy
    pub strategy: SchedulingStrategy,

    /// Priority configuration
    pub priority: PriorityConfig,

    /// Queue configuration
    pub queue: QueueConfig,

    /// Load balancing configuration
    pub load_balancing: LoadBalancingConfig,
}

/// Workflow scheduling strategies
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum SchedulingStrategy {
    /// First-In-First-Out
    Fifo,
    /// Priority-based scheduling
    Priority,
    /// Round-robin scheduling
    RoundRobin,
    /// Weighted round-robin
    WeightedRoundRobin,
    /// Shortest job first
    ShortestJobFirst,
}

/// Priority configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PriorityConfig {
    /// Default workflow priority
    pub default_priority: u8,

    /// Minimum priority value
    pub min_priority: u8,

    /// Maximum priority value
    pub max_priority: u8,

    /// Priority aging configuration
    pub aging: PriorityAgingConfig,
}

/// Priority aging configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PriorityAgingConfig {
    /// Enable priority aging
    pub enabled: bool,

    /// Aging interval
    pub interval: Duration,

    /// Priority increment per aging cycle
    pub increment: u8,

    /// Maximum priority after aging
    pub max_aged_priority: u8,
}

/// Queue configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueConfig {
    /// Maximum queue size
    pub max_size: u32,

    /// Queue overflow strategy
    pub overflow_strategy: QueueOverflowStrategy,

    /// Queue persistence
    pub persistence: bool,

    /// Queue monitoring configuration
    pub monitoring: QueueMonitoringConfig,
}

/// Queue overflow strategies
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum QueueOverflowStrategy {
    /// Reject new workflows
    Reject,
    /// Drop oldest workflows
    DropOldest,
    /// Drop lowest priority workflows
    DropLowestPriority,
    /// Spill to disk
    SpillToDisk,
}

/// Queue monitoring configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueMonitoringConfig {
    /// Enable queue metrics
    pub metrics_enabled: bool,

    /// Queue depth alert threshold
    pub depth_alert_threshold: u32,

    /// Queue age alert threshold
    pub age_alert_threshold: Duration,
}

/// Load balancing configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoadBalancingConfig {
    /// Load balancing algorithm
    pub algorithm: LoadBalancingAlgorithm,

    /// Worker selection criteria
    pub worker_selection: WorkerSelectionConfig,

    /// Health check configuration
    pub health_check: HealthCheckConfig,
}

/// Load balancing algorithms
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum LoadBalancingAlgorithm {
    /// Round-robin distribution
    RoundRobin,
    /// Least connections
    LeastConnections,
    /// Least response time
    LeastResponseTime,
    /// Resource-based (CPU, memory)
    ResourceBased,
    /// Custom algorithm
    Custom(String),
}

/// Worker selection configuration
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct WorkerSelectionConfig {
    /// Preferred worker labels
    pub preferred_labels: HashMap<String, String>,

    /// Required worker capabilities
    pub required_capabilities: Vec<String>,

    /// Avoid workers with specific labels
    pub avoid_labels: HashMap<String, String>,
}

/// Health check configuration for load balancing
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthCheckConfig {
    /// Enable health checks
    pub enabled: bool,

    /// Health check interval
    pub interval: Duration,

    /// Health check timeout
    pub timeout: Duration,

    /// Unhealthy threshold
    pub unhealthy_threshold: u32,
}

/// Workflow retry configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowRetryConfig {
    /// Enable workflow-level retries
    pub enabled: bool,

    /// Maximum workflow retry attempts
    pub max_attempts: u32,

    /// Retry delay configuration
    pub delay: RetryDelayConfig,

    /// Conditions that trigger workflow retries
    pub retry_conditions: Vec<WorkflowRetryCondition>,
}

/// Retry delay configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetryDelayConfig {
    /// Initial retry delay
    pub initial_delay: Duration,

    /// Maximum retry delay
    pub max_delay: Duration,

    /// Delay calculation strategy
    pub strategy: RetryDelayStrategy,

    /// Jitter configuration
    pub jitter: JitterConfig,
}

/// Retry delay strategies
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum RetryDelayStrategy {
    /// Fixed delay
    Fixed,
    /// Linear backoff
    Linear,
    /// Exponential backoff
    Exponential,
    /// Custom formula
    Custom(String),
}

/// Jitter configuration for retry delays
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JitterConfig {
    /// Enable jitter
    pub enabled: bool,

    /// Jitter percentage (0.0 - 1.0)
    pub percentage: f64,

    /// Jitter distribution
    pub distribution: JitterDistribution,
}

/// Jitter distribution types
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum JitterDistribution {
    /// Uniform distribution
    Uniform,
    /// Normal distribution
    Normal,
    /// Exponential distribution
    Exponential,
}

/// Workflow retry conditions
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum WorkflowRetryCondition {
    /// Retry on any failure
    AnyFailure,
    /// Retry on specific step failures
    StepFailures(Vec<String>),
    /// Retry on infrastructure failures
    InfrastructureFailure,
    /// Retry on timeout
    Timeout,
    /// Retry on resource exhaustion
    ResourceExhaustion,
}

/// Workflow limits and quotas configuration
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct WorkflowLimitsConfig {
    /// Resource limits
    pub resources: ResourceLimitsConfig,

    /// Time limits
    pub time: TimeLimitsConfig,

    /// Size limits
    pub size: SizeLimitsConfig,

    /// Rate limits
    pub rate: RateLimitsConfig,
}

/// Resource limits configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceLimitsConfig {
    /// Maximum CPU usage per workflow (millicores)
    pub max_cpu: u32,

    /// Maximum memory usage per workflow (MB)
    pub max_memory: u32,

    /// Maximum disk usage per workflow (MB)
    pub max_disk: u32,

    /// Maximum network bandwidth per workflow (Mbps)
    pub max_network: u32,
}

/// Time limits configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimeLimitsConfig {
    /// Maximum workflow execution time
    pub max_execution_time: Duration,

    /// Maximum workflow idle time
    pub max_idle_time: Duration,

    /// Maximum workflow queue time
    pub max_queue_time: Duration,

    /// Maximum step execution time
    pub max_step_time: Duration,
}

/// Size limits configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SizeLimitsConfig {
    /// Maximum workflow definition size (bytes)
    pub max_definition_size: u64,

    /// Maximum workflow input size (bytes)
    pub max_input_size: u64,

    /// Maximum workflow output size (bytes)
    pub max_output_size: u64,

    /// Maximum log size per workflow (bytes)
    pub max_log_size: u64,
}

/// Rate limits configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RateLimitsConfig {
    /// Maximum workflows per minute
    pub max_workflows_per_minute: u32,

    /// Maximum workflows per hour
    pub max_workflows_per_hour: u32,

    /// Maximum workflows per day
    pub max_workflows_per_day: u32,

    /// Rate limit enforcement strategy
    pub enforcement_strategy: RateLimitStrategy,
}

/// Rate limit enforcement strategies
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum RateLimitStrategy {
    /// Reject exceeding requests
    Reject,
    /// Queue exceeding requests
    Queue,
    /// Throttle exceeding requests
    Throttle,
}

// Default implementations
impl Default for WorkflowCoordinationConfig {
    fn default() -> Self {
        Self {
            mode: CoordinationMode::Local,
            local: LocalModeConfig::default(),
            distributed: DistributedModeConfig::default(),
        }
    }
}

impl Default for LocalModeConfig {
    fn default() -> Self {
        Self {
            max_concurrent_workflows: 100,
            persist_state: true,
            workflow_timeout_secs: 3600, // 1 hour
        }
    }
}

impl Default for DistributedModeConfig {
    fn default() -> Self {
        Self {
            workflow_stream: "orcher:workflows".to_string(),
            task_stream: "orcher:tasks".to_string(),
            consumer_group: "workflow-engine".to_string(),
            batch_size: 10,
            enable_partitioning: false,
            partition_count: 4,
        }
    }
}

impl Default for WorkflowExecutionConfig {
    fn default() -> Self {
        Self {
            max_concurrent_workflows: 100,
            default_timeout: Duration::from_secs(3600), // 1 hour
            max_timeout: Duration::from_secs(86400),    // 24 hours
            execution_mode: WorkflowExecutionMode::Mixed,
            step_execution: StepExecutionConfig::default(),
            variables: WorkflowVariablesConfig::default(),
        }
    }
}

impl Default for StepExecutionConfig {
    fn default() -> Self {
        Self {
            max_concurrent_steps: 10,
            default_step_timeout: Duration::from_secs(300), // 5 minutes
            retry: StepRetryConfig::default(),
            output_size_limit: 10 * 1024 * 1024, // 10 MB
        }
    }
}

impl Default for StepRetryConfig {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            initial_delay: Duration::from_secs(1),
            max_delay: Duration::from_secs(300),
            backoff_multiplier: 2.0,
            retry_conditions: vec![
                RetryCondition::Timeout,
                RetryCondition::ResourceUnavailable,
                RetryCondition::NetworkError,
            ],
        }
    }
}

impl Default for WorkflowVariablesConfig {
    fn default() -> Self {
        Self {
            max_variables: 1000,
            max_variable_size: 1024 * 1024, // 1 MB
            encryption: VariableEncryptionConfig::default(),
            validation: VariableValidationConfig::default(),
        }
    }
}

impl Default for VariableEncryptionConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            algorithm: "AES-256-GCM".to_string(),
            encrypt_patterns: vec![
                "*password*".to_string(),
                "*secret*".to_string(),
                "*token*".to_string(),
                "*key*".to_string(),
            ],
        }
    }
}

impl Default for VariableValidationConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            type_validation: HashMap::new(),
            constraint_validation: HashMap::new(),
        }
    }
}

impl Default for WorkflowStateConfig {
    fn default() -> Self {
        Self {
            persistence_mode: StatePersistenceMode::Database,
            checkpoints: StateCheckpointConfig::default(),
            cleanup: StateCleanupConfig::default(),
            compression: StateCompressionConfig::default(),
        }
    }
}

impl Default for StateCheckpointConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            interval: Duration::from_secs(300), // 5 minutes
            on_step_completion: true,
            max_history: 10,
        }
    }
}

impl Default for StateCleanupConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            completed_after: Duration::from_secs(86400 * 7), // 7 days
            failed_after: Duration::from_secs(86400 * 30),   // 30 days
            cancelled_after: Duration::from_secs(86400 * 3), // 3 days
            orphaned_after: Duration::from_secs(86400),      // 1 day
        }
    }
}

impl Default for StateCompressionConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            algorithm: "gzip".to_string(),
            level: 6,
            min_size: 1024, // 1 KB
        }
    }
}

impl Default for WorkflowSchedulingConfig {
    fn default() -> Self {
        Self {
            strategy: SchedulingStrategy::Priority,
            priority: PriorityConfig::default(),
            queue: QueueConfig::default(),
            load_balancing: LoadBalancingConfig::default(),
        }
    }
}

impl Default for PriorityConfig {
    fn default() -> Self {
        Self {
            default_priority: 50,
            min_priority: 0,
            max_priority: 100,
            aging: PriorityAgingConfig::default(),
        }
    }
}

impl Default for PriorityAgingConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            interval: Duration::from_secs(3600), // 1 hour
            increment: 1,
            max_aged_priority: 90,
        }
    }
}

impl Default for QueueConfig {
    fn default() -> Self {
        Self {
            max_size: 10000,
            overflow_strategy: QueueOverflowStrategy::Reject,
            persistence: true,
            monitoring: QueueMonitoringConfig::default(),
        }
    }
}

impl Default for QueueMonitoringConfig {
    fn default() -> Self {
        Self {
            metrics_enabled: true,
            depth_alert_threshold: 5000,
            age_alert_threshold: Duration::from_secs(3600), // 1 hour
        }
    }
}

impl Default for LoadBalancingConfig {
    fn default() -> Self {
        Self {
            algorithm: LoadBalancingAlgorithm::ResourceBased,
            worker_selection: WorkerSelectionConfig::default(),
            health_check: HealthCheckConfig::default(),
        }
    }
}

impl Default for HealthCheckConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            interval: Duration::from_secs(30),
            timeout: Duration::from_secs(5),
            unhealthy_threshold: 3,
        }
    }
}

impl Default for WorkflowRetryConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_attempts: 3,
            delay: RetryDelayConfig::default(),
            retry_conditions: vec![
                WorkflowRetryCondition::InfrastructureFailure,
                WorkflowRetryCondition::ResourceExhaustion,
            ],
        }
    }
}

impl Default for RetryDelayConfig {
    fn default() -> Self {
        Self {
            initial_delay: Duration::from_secs(30),
            max_delay: Duration::from_secs(3600), // 1 hour
            strategy: RetryDelayStrategy::Exponential,
            jitter: JitterConfig::default(),
        }
    }
}

impl Default for JitterConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            percentage: 0.1, // 10% jitter
            distribution: JitterDistribution::Uniform,
        }
    }
}

impl Default for ResourceLimitsConfig {
    fn default() -> Self {
        Self {
            max_cpu: 1000,    // 1 CPU core
            max_memory: 1024, // 1 GB
            max_disk: 10240,  // 10 GB
            max_network: 100, // 100 Mbps
        }
    }
}

impl Default for TimeLimitsConfig {
    fn default() -> Self {
        Self {
            max_execution_time: Duration::from_secs(86400), // 24 hours
            max_idle_time: Duration::from_secs(3600),       // 1 hour
            max_queue_time: Duration::from_secs(1800),      // 30 minutes
            max_step_time: Duration::from_secs(3600),       // 1 hour
        }
    }
}

impl Default for SizeLimitsConfig {
    fn default() -> Self {
        Self {
            max_definition_size: 1024 * 1024,  // 1 MB
            max_input_size: 10 * 1024 * 1024,  // 10 MB
            max_output_size: 10 * 1024 * 1024, // 10 MB
            max_log_size: 100 * 1024 * 1024,   // 100 MB
        }
    }
}

impl Default for RateLimitsConfig {
    fn default() -> Self {
        Self {
            max_workflows_per_minute: 100,
            max_workflows_per_hour: 1000,
            max_workflows_per_day: 10000,
            enforcement_strategy: RateLimitStrategy::Queue,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_workflow_config_default() {
        let config = WorkflowConfig::default();

        assert_eq!(config.execution.max_concurrent_workflows, 100);
        assert_eq!(
            config.execution.execution_mode,
            WorkflowExecutionMode::Mixed
        );
        assert_eq!(
            config.state.persistence_mode,
            StatePersistenceMode::Database
        );
        assert_eq!(config.scheduling.strategy, SchedulingStrategy::Priority);
    }

    #[test]
    fn test_workflow_execution_config() {
        let config = WorkflowExecutionConfig::default();

        assert_eq!(config.max_concurrent_workflows, 100);
        assert_eq!(config.default_timeout, Duration::from_secs(3600));
        assert_eq!(config.execution_mode, WorkflowExecutionMode::Mixed);
        assert_eq!(config.step_execution.max_concurrent_steps, 10);
    }

    #[test]
    fn test_retry_conditions() {
        let conditions = [
            RetryCondition::AnyError,
            RetryCondition::Timeout,
            RetryCondition::NetworkError,
        ];

        assert_eq!(conditions.len(), 3);
        assert!(conditions.contains(&RetryCondition::Timeout));
    }

    #[test]
    fn test_scheduling_strategies() {
        let strategies = vec![
            SchedulingStrategy::Fifo,
            SchedulingStrategy::Priority,
            SchedulingStrategy::RoundRobin,
        ];

        for strategy in strategies {
            let json = serde_json::to_string(&strategy).unwrap();
            let deserialized: SchedulingStrategy = serde_json::from_str(&json).unwrap();
            assert_eq!(strategy, deserialized);
        }
    }

    #[test]
    fn test_config_serialization() {
        let config = WorkflowConfig::default();

        let json = serde_json::to_string(&config).unwrap();
        assert!(json.contains("execution"));
        assert!(json.contains("scheduling"));
        assert!(json.contains("retry"));

        let deserialized: WorkflowConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(
            config.execution.max_concurrent_workflows,
            deserialized.execution.max_concurrent_workflows
        );
    }

    #[test]
    fn test_load_balancing_algorithms() {
        let algorithms = vec![
            LoadBalancingAlgorithm::RoundRobin,
            LoadBalancingAlgorithm::LeastConnections,
            LoadBalancingAlgorithm::ResourceBased,
        ];

        for algorithm in algorithms {
            let json = serde_json::to_string(&algorithm).unwrap();
            let deserialized: LoadBalancingAlgorithm = serde_json::from_str(&json).unwrap();
            assert_eq!(algorithm, deserialized);
        }
    }

    #[test]
    fn test_variable_encryption_config() {
        let config = VariableEncryptionConfig::default();

        assert!(config.enabled);
        assert_eq!(config.algorithm, "AES-256-GCM");
        assert!(config.encrypt_patterns.contains(&"*password*".to_string()));
        assert!(config.encrypt_patterns.contains(&"*secret*".to_string()));
    }

    #[test]
    fn test_state_cleanup_config() {
        let config = StateCleanupConfig::default();

        assert!(config.enabled);
        assert_eq!(config.completed_after, Duration::from_secs(86400 * 7));
        assert_eq!(config.failed_after, Duration::from_secs(86400 * 30));
    }

    #[test]
    fn test_resource_limits() {
        let config = ResourceLimitsConfig::default();

        assert_eq!(config.max_cpu, 1000); // 1 core
        assert_eq!(config.max_memory, 1024); // 1 GB
        assert_eq!(config.max_disk, 10240); // 10 GB
    }

    #[test]
    fn test_queue_overflow_strategies() {
        let strategies = vec![
            QueueOverflowStrategy::Reject,
            QueueOverflowStrategy::DropOldest,
            QueueOverflowStrategy::SpillToDisk,
        ];

        for strategy in strategies {
            let json = serde_json::to_string(&strategy).unwrap();
            let deserialized: QueueOverflowStrategy = serde_json::from_str(&json).unwrap();
            assert_eq!(strategy, deserialized);
        }
    }

    #[test]
    fn test_coordination_mode_default() {
        let mode = CoordinationMode::default();
        assert_eq!(mode, CoordinationMode::Local);
    }

    #[test]
    fn test_coordination_mode_serialization() {
        let local = CoordinationMode::Local;
        let json = serde_json::to_string(&local).unwrap();
        assert_eq!(json, r#""local""#);

        let distributed = CoordinationMode::Distributed;
        let json = serde_json::to_string(&distributed).unwrap();
        assert_eq!(json, r#""distributed""#);
    }

    #[test]
    fn test_local_mode_config_default() {
        let config = LocalModeConfig::default();
        assert_eq!(config.max_concurrent_workflows, 100);
        assert!(config.persist_state);
        assert_eq!(config.workflow_timeout_secs, 3600);
    }

    #[test]
    fn test_distributed_mode_config_default() {
        let config = DistributedModeConfig::default();
        assert_eq!(config.workflow_stream, "orcher:workflows");
        assert_eq!(config.task_stream, "orcher:tasks");
        assert_eq!(config.consumer_group, "workflow-engine");
        assert_eq!(config.batch_size, 10);
        assert!(!config.enable_partitioning);
        assert_eq!(config.partition_count, 4);
    }

    #[test]
    fn test_workflow_coordination_config_default() {
        let config = WorkflowCoordinationConfig::default();
        assert_eq!(config.mode, CoordinationMode::Local);
        assert_eq!(config.local.max_concurrent_workflows, 100);
        assert_eq!(config.distributed.batch_size, 10);
    }
}
