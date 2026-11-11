//! Core orchestration types.

use jiff::SignedDuration;
use orcher_common::types::identifiers::{TaskExecutionId, TaskId, UserId, WorkerId};
use orcher_common::types::task::TaskStatus;
use orcher_common::types::time::Timestamptz;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::net::SocketAddr;

/// Task priority for scheduling
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum TaskPriority {
    /// Lowest priority
    Low = 0,
    /// Normal priority (default)
    Normal = 1,
    /// High priority
    High = 2,
    /// Critical priority
    Critical = 3,
}

impl Default for TaskPriority {
    fn default() -> Self {
        Self::Normal
    }
}

/// Worker capability description
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerCapability {
    /// Capability name (e.g., "python", "gpu", "docker")
    pub name: String,
    /// Capability version (optional)
    pub version: Option<String>,
}

/// Resource requirements for a task
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct ResourceRequirements {
    /// Minimum CPU cores required
    pub min_cpu_cores: Option<u32>,
    /// Minimum memory in MB
    pub min_memory_mb: Option<u64>,
    /// Required capabilities
    pub capabilities: Vec<WorkerCapability>,
    /// Custom tags for worker selection
    pub tags: HashMap<String, String>,
}

/// Task submission request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskSubmission {
    /// Task ID
    pub task_id: TaskId,
    /// Task name
    pub name: String,
    /// Task type (e.g., "shell", "python", "docker")
    pub task_type: String,
    /// Task payload
    pub payload: serde_json::Value,
    /// Task priority
    pub priority: TaskPriority,
    /// Resource requirements
    pub resources: ResourceRequirements,
    /// Maximum execution timeout in seconds
    pub timeout_seconds: Option<u64>,
    /// Maximum retry attempts
    pub max_retries: u32,
    /// Tags for categorization and filtering
    pub tags: HashMap<String, String>,
    /// Submission timestamp
    pub submitted_at: Timestamptz,
}

/// Worker registration information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerRegistration {
    /// Worker ID
    pub worker_id: WorkerId,
    /// Worker name
    pub name: String,
    /// Worker capabilities
    pub capabilities: Vec<WorkerCapability>,
    /// Available CPU cores
    pub cpu_cores: u32,
    /// Available memory in MB
    pub memory_mb: u64,
    /// Worker tags
    pub tags: HashMap<String, String>,
    /// Worker endpoint for communication
    pub endpoint: String,
    /// Registration timestamp
    pub registered_at: Timestamptz,
}

/// Worker health status
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum WorkerHealthStatus {
    /// Worker is healthy and available
    Healthy,
    /// Worker is unhealthy but still registered
    Unhealthy,
    /// Worker is draining (not accepting new tasks)
    Draining,
    /// Worker is offline
    Offline,
}

/// Worker status information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerStatus {
    /// Worker ID
    pub worker_id: WorkerId,
    /// Health status
    pub health: WorkerHealthStatus,
    /// Number of active tasks
    pub active_tasks: u32,
    /// Current CPU usage percentage (0-100)
    pub cpu_usage_percent: f32,
    /// Current memory usage in MB
    pub memory_usage_mb: u64,
    /// Last heartbeat timestamp
    pub last_heartbeat: Timestamptz,
}

/// Coordination state
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum OrchestrationState {
    /// Orchestrator is initializing
    Initializing,
    /// Orchestrator is running and accepting tasks
    Running,
    /// Orchestrator is draining (completing existing tasks)
    Draining,
    /// Orchestrator is paused
    Paused,
    /// Orchestrator is shutting down
    ShuttingDown,
}

/// Statistics for monitoring
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct OrchestrationStats {
    /// Total tasks submitted
    pub total_submitted: u64,
    /// Total tasks completed
    pub total_completed: u64,
    /// Total tasks failed
    pub total_failed: u64,
    /// Total tasks cancelled
    pub total_cancelled: u64,
    /// Current pending tasks
    pub pending: u64,
    /// Current running tasks
    pub running: u64,
    /// Total registered workers
    pub workers_registered: u64,
    /// Currently active workers
    pub workers_active: u64,
}

// ================================================================================================
// EXECUTION TYPES
// ================================================================================================

/// Task execution context
///
/// Contains all information needed to execute a task including security context,
/// resource allocation, and execution parameters.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskExecutionContext {
    /// Task execution ID
    pub execution_id: TaskExecutionId,
    /// Task ID
    pub task_id: TaskId,
    /// Worker ID assigned to execute the task
    pub worker_id: WorkerId,
    /// User who submitted the task
    pub submitted_by: Option<UserId>,
    /// Security context for execution
    pub security_context: SecurityContext,
    /// Resource allocation for this execution
    pub resource_allocation: ResourceAllocation,
    /// Execution timeout
    pub timeout: SignedDuration,
    /// Retry attempt number (0 for first attempt)
    pub retry_attempt: u32,
    /// Parent execution ID (for retries or child tasks)
    pub parent_execution_id: Option<TaskExecutionId>,
    /// Execution started at
    pub started_at: Timestamptz,
}

/// Security context for task execution
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SecurityContext {
    /// User ID
    pub user_id: Option<UserId>,
    /// User roles
    pub roles: Vec<String>,
    /// Permissions
    pub permissions: Vec<String>,
    /// Authentication token (redacted in logs)
    #[serde(skip_serializing)]
    #[allow(dead_code)]
    pub auth_token: Option<String>,
}

/// Resource allocation for task execution
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceAllocation {
    /// Allocated CPU cores
    #[serde(default = "default_cpu_cores")]
    pub cpu_cores: u32,
    /// Allocated memory in MB
    #[serde(default = "default_memory_mb")]
    pub memory_mb: u64,
    /// Allocated disk space in MB
    pub disk_mb: Option<u64>,
    /// Allocated GPU count
    pub gpu_count: Option<u32>,
    /// Network bandwidth limit in Mbps
    pub network_mbps: Option<u32>,
    /// Custom resource allocations
    pub custom_resources: HashMap<String, String>,
}

impl Default for ResourceAllocation {
    fn default() -> Self {
        Self {
            cpu_cores: 1,
            memory_mb: 512,
            disk_mb: None,
            gpu_count: None,
            network_mbps: None,
            custom_resources: HashMap::new(),
        }
    }
}

/// Task execution result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskExecutionResult {
    /// Execution ID
    pub execution_id: TaskExecutionId,
    /// Task ID
    pub task_id: TaskId,
    /// Final status
    pub status: TaskStatus,
    /// Output data
    pub output: Option<serde_json::Value>,
    /// Error message if failed
    pub error: Option<String>,
    /// Exit code
    pub exit_code: Option<i32>,
    /// Execution duration
    pub duration: SignedDuration,
    /// Started at timestamp
    pub started_at: Timestamptz,
    /// Completed at timestamp
    pub completed_at: Timestamptz,
    /// Resource usage statistics
    pub resource_usage: ResourceUsage,
}

/// Helper function for default CPU cores
fn default_cpu_cores() -> u32 {
    1
}

/// Helper function for default memory MB
fn default_memory_mb() -> u64 {
    512
}

/// Resource usage statistics
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ResourceUsage {
    /// Peak CPU usage percentage
    pub peak_cpu_percent: f32,
    /// Average CPU usage percentage
    pub avg_cpu_percent: f32,
    /// Peak memory usage in MB
    pub peak_memory_mb: u64,
    /// Average memory usage in MB
    pub avg_memory_mb: u64,
    /// Disk I/O read in bytes
    pub disk_read_bytes: u64,
    /// Disk I/O write in bytes
    pub disk_write_bytes: u64,
    /// Network I/O received in bytes
    pub network_rx_bytes: u64,
    /// Network I/O transmitted in bytes
    pub network_tx_bytes: u64,
}

// ================================================================================================
// STATE TRANSITION TYPES
// ================================================================================================

/// State transition event
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StateTransition {
    /// Task ID
    pub task_id: TaskId,
    /// Previous state
    pub from_state: TaskStatus,
    /// New state
    pub to_state: TaskStatus,
    /// Transition timestamp
    pub transitioned_at: Timestamptz,
    /// Reason for transition
    pub reason: Option<String>,
    /// Worker ID involved in transition
    pub worker_id: Option<WorkerId>,
}

/// Coordination state transition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrchestrationStateTransition {
    /// Previous state
    pub from_state: OrchestrationState,
    /// New state
    pub to_state: OrchestrationState,
    /// Transition timestamp
    pub transitioned_at: Timestamptz,
    /// Reason for transition
    pub reason: String,
}

// ================================================================================================
// ENHANCED WORKER TYPES
// ================================================================================================

/// Worker metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerMetadata {
    /// Worker ID
    pub worker_id: WorkerId,
    /// Worker hostname
    pub hostname: String,
    /// Worker IP address
    pub ip_address: Option<SocketAddr>,
    /// Worker version
    pub version: String,
    /// Operating system
    pub os: String,
    /// Architecture
    pub arch: String,
    /// Started at timestamp
    pub started_at: Timestamptz,
    /// Custom metadata
    pub custom: HashMap<String, String>,
}

/// Worker pool information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerPool {
    /// Pool name
    pub name: String,
    /// Pool description
    pub description: Option<String>,
    /// Workers in this pool
    pub worker_ids: Vec<WorkerId>,
    /// Pool capacity limits
    pub capacity: PoolCapacity,
    /// Pool tags
    pub tags: HashMap<String, String>,
}

/// Pool capacity configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PoolCapacity {
    /// Minimum number of workers
    pub min_workers: u32,
    /// Maximum number of workers
    pub max_workers: u32,
    /// Current number of workers
    pub current_workers: u32,
    /// Target number of workers
    pub target_workers: u32,
}

// ================================================================================================
// ENHANCED RESOURCE TYPES
// ================================================================================================

/// Resource quota
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceQuota {
    /// User or tenant ID
    pub owner_id: String,
    /// Maximum CPU cores
    pub max_cpu_cores: u32,
    /// Maximum memory in MB
    pub max_memory_mb: u64,
    /// Maximum concurrent tasks
    pub max_concurrent_tasks: u32,
    /// Maximum tasks per hour
    pub max_tasks_per_hour: u32,
    /// Current usage
    pub current_usage: ResourceUsage,
}

/// Resource pool
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourcePool {
    /// Pool name
    pub name: String,
    /// Total available CPU cores
    pub total_cpu_cores: u32,
    /// Total available memory in MB
    pub total_memory_mb: u64,
    /// Currently allocated CPU cores
    pub allocated_cpu_cores: u32,
    /// Currently allocated memory in MB
    pub allocated_memory_mb: u64,
    /// Available CPU cores
    pub available_cpu_cores: u32,
    /// Available memory in MB
    pub available_memory_mb: u64,
}

impl ResourcePool {
    /// Calculate available resources
    pub fn calculate_available(&mut self) {
        self.available_cpu_cores = self
            .total_cpu_cores
            .saturating_sub(self.allocated_cpu_cores);
        self.available_memory_mb = self
            .total_memory_mb
            .saturating_sub(self.allocated_memory_mb);
    }

    /// Check if resources are available
    pub fn has_available(&self, cpu_cores: u32, memory_mb: u64) -> bool {
        self.available_cpu_cores >= cpu_cores && self.available_memory_mb >= memory_mb
    }
}

// ================================================================================================
// COORDINATION METRICS
// ================================================================================================

/// Detailed coordination metrics
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct OrchestrationMetrics {
    /// Task metrics
    pub tasks: TaskMetrics,
    /// Worker metrics
    pub workers: WorkerMetrics,
    /// Resource metrics
    pub resources: ResourceMetrics,
    /// Performance metrics
    pub performance: PerformanceMetrics,
}

/// Task-specific metrics
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TaskMetrics {
    /// Total tasks submitted
    pub total_submitted: u64,
    /// Total tasks completed successfully
    pub total_completed: u64,
    /// Total tasks failed
    pub total_failed: u64,
    /// Total tasks cancelled
    pub total_cancelled: u64,
    /// Total tasks timed out
    pub total_timed_out: u64,
    /// Current pending tasks
    pub pending: u64,
    /// Current running tasks
    pub running: u64,
    /// Current queued tasks
    pub queued: u64,
    /// Average execution time in seconds
    pub avg_execution_time_secs: f64,
    /// P95 execution time in seconds
    pub p95_execution_time_secs: f64,
    /// P99 execution time in seconds
    pub p99_execution_time_secs: f64,
}

/// Worker-specific metrics
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct WorkerMetrics {
    /// Total workers registered
    pub total_registered: u64,
    /// Currently active workers
    pub active: u64,
    /// Currently idle workers
    pub idle: u64,
    /// Currently busy workers
    pub busy: u64,
    /// Currently draining workers
    pub draining: u64,
    /// Currently unhealthy workers
    pub unhealthy: u64,
    /// Average worker utilization percentage
    pub avg_utilization_percent: f32,
}

/// Resource-specific metrics
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ResourceMetrics {
    /// Total CPU cores available
    pub total_cpu_cores: u32,
    /// Currently allocated CPU cores
    pub allocated_cpu_cores: u32,
    /// Total memory in MB available
    pub total_memory_mb: u64,
    /// Currently allocated memory in MB
    pub allocated_memory_mb: u64,
    /// Resource utilization percentage
    pub utilization_percent: f32,
}

/// Performance-specific metrics
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PerformanceMetrics {
    /// Average task submission latency in milliseconds
    pub avg_submission_latency_ms: f64,
    /// Average task dispatch latency in milliseconds
    pub avg_dispatch_latency_ms: f64,
    /// Throughput (tasks per second)
    pub throughput_tps: f64,
    /// Queue depth
    pub queue_depth: u64,
}

// ================================================================================================
// TASK BUILDER
// ================================================================================================

/// Builder for creating task submissions
#[derive(Debug, Clone)]
pub struct TaskSubmissionBuilder {
    task_id: Option<TaskId>,
    name: String,
    task_type: String,
    payload: serde_json::Value,
    priority: TaskPriority,
    resources: ResourceRequirements,
    timeout_seconds: Option<u64>,
    max_retries: u32,
    tags: HashMap<String, String>,
}

impl TaskSubmissionBuilder {
    /// Create a new task submission builder
    pub fn new(name: impl Into<String>, task_type: impl Into<String>) -> Self {
        Self {
            task_id: None,
            name: name.into(),
            task_type: task_type.into(),
            payload: serde_json::Value::Null,
            priority: TaskPriority::default(),
            resources: ResourceRequirements::default(),
            timeout_seconds: None,
            max_retries: 3,
            tags: HashMap::new(),
        }
    }

    /// Set task ID
    pub fn with_task_id(mut self, task_id: TaskId) -> Self {
        self.task_id = Some(task_id);
        self
    }

    /// Set task payload
    pub fn with_payload(mut self, payload: serde_json::Value) -> Self {
        self.payload = payload;
        self
    }

    /// Set task priority
    pub fn with_priority(mut self, priority: TaskPriority) -> Self {
        self.priority = priority;
        self
    }

    /// Set resource requirements
    pub fn with_resources(mut self, resources: ResourceRequirements) -> Self {
        self.resources = resources;
        self
    }

    /// Set timeout in seconds
    pub fn with_timeout(mut self, timeout_seconds: u64) -> Self {
        self.timeout_seconds = Some(timeout_seconds);
        self
    }

    /// Set max retries
    pub fn with_max_retries(mut self, max_retries: u32) -> Self {
        self.max_retries = max_retries;
        self
    }

    /// Add a tag
    pub fn with_tag(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.tags.insert(key.into(), value.into());
        self
    }

    /// Build the task submission
    pub fn build(self) -> TaskSubmission {
        TaskSubmission {
            task_id: self.task_id.unwrap_or_else(TaskId::new_v4),
            name: self.name,
            task_type: self.task_type,
            payload: self.payload,
            priority: self.priority,
            resources: self.resources,
            timeout_seconds: self.timeout_seconds,
            max_retries: self.max_retries,
            tags: self.tags,
            submitted_at: Timestamptz::now(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_task_priority_ordering() {
        assert!(TaskPriority::Critical > TaskPriority::High);
        assert!(TaskPriority::High > TaskPriority::Normal);
        assert!(TaskPriority::Normal > TaskPriority::Low);
    }

    #[test]
    fn test_task_priority_default() {
        assert_eq!(TaskPriority::default(), TaskPriority::Normal);
    }

    #[test]
    fn test_worker_capability_creation() {
        let cap = WorkerCapability {
            name: "python".to_string(),
            version: Some("3.9".to_string()),
        };
        assert_eq!(cap.name, "python");
        assert_eq!(cap.version, Some("3.9".to_string()));
    }

    #[test]
    fn test_resource_requirements_default() {
        let resources = ResourceRequirements::default();
        assert_eq!(resources.min_cpu_cores, None);
        assert_eq!(resources.min_memory_mb, None);
        assert!(resources.capabilities.is_empty());
        assert!(resources.tags.is_empty());
    }

    #[test]
    fn test_coordination_stats_default() {
        let stats = OrchestrationStats::default();
        assert_eq!(stats.total_submitted, 0);
        assert_eq!(stats.pending, 0);
        assert_eq!(stats.workers_registered, 0);
    }

    #[test]
    fn test_security_context_default() {
        let ctx = SecurityContext::default();
        assert!(ctx.user_id.is_none());
        assert!(ctx.roles.is_empty());
        assert!(ctx.permissions.is_empty());
    }

    #[test]
    fn test_resource_allocation_default() {
        let alloc = ResourceAllocation::default();
        assert_eq!(alloc.cpu_cores, 1);
        assert_eq!(alloc.memory_mb, 512);
        assert_eq!(alloc.disk_mb, None);
        assert_eq!(alloc.gpu_count, None);
        assert_eq!(alloc.network_mbps, None);
        assert!(alloc.custom_resources.is_empty());
    }

    #[test]
    fn test_resource_pool_calculate_available() {
        let mut pool = ResourcePool {
            name: "test-pool".to_string(),
            total_cpu_cores: 10,
            total_memory_mb: 10240,
            allocated_cpu_cores: 3,
            allocated_memory_mb: 2048,
            available_cpu_cores: 0,
            available_memory_mb: 0,
        };

        pool.calculate_available();
        assert_eq!(pool.available_cpu_cores, 7);
        assert_eq!(pool.available_memory_mb, 8192);
    }

    #[test]
    fn test_resource_pool_has_available() {
        let pool = ResourcePool {
            name: "test-pool".to_string(),
            total_cpu_cores: 10,
            total_memory_mb: 10240,
            allocated_cpu_cores: 3,
            allocated_memory_mb: 2048,
            available_cpu_cores: 7,
            available_memory_mb: 8192,
        };

        assert!(pool.has_available(4, 4096));
        assert!(!pool.has_available(8, 4096));
        assert!(!pool.has_available(4, 9000));
    }

    #[test]
    fn test_task_submission_builder() {
        let submission = TaskSubmissionBuilder::new("test-task", "shell")
            .with_priority(TaskPriority::High)
            .with_timeout(300)
            .with_max_retries(5)
            .with_tag("env", "production")
            .build();

        assert_eq!(submission.name, "test-task");
        assert_eq!(submission.task_type, "shell");
        assert_eq!(submission.priority, TaskPriority::High);
        assert_eq!(submission.timeout_seconds, Some(300));
        assert_eq!(submission.max_retries, 5);
        assert_eq!(submission.tags.get("env"), Some(&"production".to_string()));
    }

    #[test]
    fn test_coordination_metrics_default() {
        let metrics = OrchestrationMetrics::default();
        assert_eq!(metrics.tasks.total_submitted, 0);
        assert_eq!(metrics.workers.active, 0);
        assert_eq!(metrics.resources.total_cpu_cores, 0);
    }

    #[test]
    fn test_resource_usage_default() {
        let usage = ResourceUsage::default();
        assert_eq!(usage.peak_cpu_percent, 0.0);
        assert_eq!(usage.peak_memory_mb, 0);
    }
}
