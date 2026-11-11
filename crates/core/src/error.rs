//! Core error types.

use orcher_common::error::OrcherError;
use orcher_common::types::time::Timestamptz;
use std::fmt;

/// Result type for core orchestration operations
pub type CoreResult<T> = Result<T, CoreError>;

/// Error recovery strategy
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecoveryStrategy {
    /// Retry the operation
    Retry,
    /// Retry with exponential backoff
    RetryWithBackoff,
    /// Fail over to alternative resource
    Failover,
    /// Abort the operation
    Abort,
    /// Ignore the error and continue
    Ignore,
    /// Manual intervention required
    Manual,
}

/// Error severity level
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum ErrorSeverity {
    /// Low severity - informational
    Low,
    /// Medium severity - warning
    Medium,
    /// High severity - error
    High,
    /// Critical severity - system failure
    Critical,
}

/// Error context for debugging and tracing
#[derive(Debug, Clone)]
pub struct ErrorContext {
    /// Error occurred at timestamp
    pub timestamp: Timestamptz,
    /// Component where error occurred
    pub component: String,
    /// Operation being performed
    pub operation: String,
    /// Additional context data
    pub context: std::collections::HashMap<String, String>,
    /// Error severity
    pub severity: ErrorSeverity,
    /// Suggested recovery strategy
    pub recovery: RecoveryStrategy,
}

impl ErrorContext {
    /// Create a new error context
    pub fn new(component: impl Into<String>, operation: impl Into<String>) -> Self {
        Self {
            timestamp: Timestamptz::now(),
            component: component.into(),
            operation: operation.into(),
            context: std::collections::HashMap::new(),
            severity: ErrorSeverity::High,
            recovery: RecoveryStrategy::Abort,
        }
    }

    /// Add context data
    pub fn with_context(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.context.insert(key.into(), value.into());
        self
    }

    /// Set severity
    pub fn with_severity(mut self, severity: ErrorSeverity) -> Self {
        self.severity = severity;
        self
    }

    /// Set recovery strategy
    pub fn with_recovery(mut self, recovery: RecoveryStrategy) -> Self {
        self.recovery = recovery;
        self
    }
}

/// Core orchestration errors
#[derive(Debug, Clone)]
pub enum CoreError {
    /// Coordination errors
    Coordination(CoordinationError),

    /// Worker management errors
    Worker(WorkerError),

    /// Task management errors
    Task(TaskError),

    /// Resource allocation errors
    Resource(ResourceError),

    /// Integration errors
    Integration(IntegrationError),

    /// Event sourcing errors
    Event(EventError),

    /// Storage errors
    Storage(StorageError),

    /// Configuration errors
    Configuration(String),

    /// Operation timed out
    Timeout(String),

    /// Circuit breaker is open, preventing execution
    CircuitBreakerOpen(String),

    /// Resource or entity not found
    NotFound(String),

    /// Validation failed
    ValidationFailed(String),

    /// Feature or functionality not yet implemented
    NotImplemented(String),

    /// Generic internal error
    Internal(String),
}

/// Coordination-specific errors
#[derive(Debug, Clone)]
pub enum CoordinationError {
    /// Task submission failed
    SubmissionFailed(String),

    /// Task processing failed
    ProcessingFailed(String),

    /// Task dispatching failed
    DispatchFailed(String),

    /// Invalid coordination state
    InvalidState(String),

    /// Coordination timeout
    Timeout(String),

    /// Queue full
    QueueFull(String),

    /// Coordination deadlock detected
    Deadlock(String),

    /// State conflict
    StateConflict(String),
}

impl CoordinationError {
    /// Get error severity
    pub fn severity(&self) -> ErrorSeverity {
        match self {
            Self::SubmissionFailed(_) => ErrorSeverity::High,
            Self::ProcessingFailed(_) => ErrorSeverity::High,
            Self::DispatchFailed(_) => ErrorSeverity::High,
            Self::InvalidState(_) => ErrorSeverity::Medium,
            Self::Timeout(_) => ErrorSeverity::Medium,
            Self::QueueFull(_) => ErrorSeverity::Medium,
            Self::Deadlock(_) => ErrorSeverity::Critical,
            Self::StateConflict(_) => ErrorSeverity::High,
        }
    }

    /// Get suggested recovery strategy
    pub fn recovery_strategy(&self) -> RecoveryStrategy {
        match self {
            Self::SubmissionFailed(_) => RecoveryStrategy::Retry,
            Self::ProcessingFailed(_) => RecoveryStrategy::Retry,
            Self::DispatchFailed(_) => RecoveryStrategy::Failover,
            Self::InvalidState(_) => RecoveryStrategy::Abort,
            Self::Timeout(_) => RecoveryStrategy::RetryWithBackoff,
            Self::QueueFull(_) => RecoveryStrategy::RetryWithBackoff,
            Self::Deadlock(_) => RecoveryStrategy::Manual,
            Self::StateConflict(_) => RecoveryStrategy::Retry,
        }
    }

    /// Check if error is retryable
    pub fn is_retryable(&self) -> bool {
        matches!(
            self.recovery_strategy(),
            RecoveryStrategy::Retry | RecoveryStrategy::RetryWithBackoff
        )
    }
}

/// Worker management errors
#[derive(Debug, Clone)]
pub enum WorkerError {
    /// Worker not found
    NotFound(String),

    /// Worker registration failed
    RegistrationFailed(String),

    /// Worker unavailable
    Unavailable(String),

    /// Worker health check failed
    HealthCheckFailed(String),

    /// No suitable worker found
    NoSuitableWorker(String),

    /// Worker capacity exceeded
    CapacityExceeded(String),

    /// Worker communication failed
    CommunicationFailed(String),

    /// Worker pool exhausted
    PoolExhausted(String),

    /// Worker disconnected
    Disconnected(String),
}

impl WorkerError {
    /// Get error severity
    pub fn severity(&self) -> ErrorSeverity {
        match self {
            Self::NotFound(_) => ErrorSeverity::Medium,
            Self::RegistrationFailed(_) => ErrorSeverity::High,
            Self::Unavailable(_) => ErrorSeverity::Medium,
            Self::HealthCheckFailed(_) => ErrorSeverity::Medium,
            Self::NoSuitableWorker(_) => ErrorSeverity::High,
            Self::CapacityExceeded(_) => ErrorSeverity::Medium,
            Self::CommunicationFailed(_) => ErrorSeverity::High,
            Self::PoolExhausted(_) => ErrorSeverity::Critical,
            Self::Disconnected(_) => ErrorSeverity::Medium,
        }
    }

    /// Get suggested recovery strategy
    pub fn recovery_strategy(&self) -> RecoveryStrategy {
        match self {
            Self::NotFound(_) => RecoveryStrategy::Abort,
            Self::RegistrationFailed(_) => RecoveryStrategy::Retry,
            Self::Unavailable(_) => RecoveryStrategy::Failover,
            Self::HealthCheckFailed(_) => RecoveryStrategy::Failover,
            Self::NoSuitableWorker(_) => RecoveryStrategy::RetryWithBackoff,
            Self::CapacityExceeded(_) => RecoveryStrategy::RetryWithBackoff,
            Self::CommunicationFailed(_) => RecoveryStrategy::Retry,
            Self::PoolExhausted(_) => RecoveryStrategy::Manual,
            Self::Disconnected(_) => RecoveryStrategy::Failover,
        }
    }

    /// Check if error is retryable
    pub fn is_retryable(&self) -> bool {
        matches!(
            self.recovery_strategy(),
            RecoveryStrategy::Retry | RecoveryStrategy::RetryWithBackoff
        )
    }
}

/// Task management errors
#[derive(Debug, Clone)]
pub enum TaskError {
    /// Task not found
    NotFound(String),

    /// Task validation failed
    ValidationFailed(String),

    /// Invalid task state
    InvalidState(String),

    /// Task already exists
    AlreadyExists(String),

    /// Task timeout
    Timeout(String),

    /// Task cancelled
    Cancelled(String),

    /// Task dependency failure
    DependencyFailed(String),

    /// Task execution failed
    ExecutionFailed(String),
}

impl TaskError {
    /// Get error severity
    pub fn severity(&self) -> ErrorSeverity {
        match self {
            Self::NotFound(_) => ErrorSeverity::Medium,
            Self::ValidationFailed(_) => ErrorSeverity::Low,
            Self::InvalidState(_) => ErrorSeverity::Medium,
            Self::AlreadyExists(_) => ErrorSeverity::Low,
            Self::Timeout(_) => ErrorSeverity::Medium,
            Self::Cancelled(_) => ErrorSeverity::Low,
            Self::DependencyFailed(_) => ErrorSeverity::High,
            Self::ExecutionFailed(_) => ErrorSeverity::High,
        }
    }

    /// Get suggested recovery strategy
    pub fn recovery_strategy(&self) -> RecoveryStrategy {
        match self {
            Self::NotFound(_) => RecoveryStrategy::Abort,
            Self::ValidationFailed(_) => RecoveryStrategy::Abort,
            Self::InvalidState(_) => RecoveryStrategy::Abort,
            Self::AlreadyExists(_) => RecoveryStrategy::Ignore,
            Self::Timeout(_) => RecoveryStrategy::Retry,
            Self::Cancelled(_) => RecoveryStrategy::Abort,
            Self::DependencyFailed(_) => RecoveryStrategy::Abort,
            Self::ExecutionFailed(_) => RecoveryStrategy::Retry,
        }
    }

    /// Check if error is retryable
    pub fn is_retryable(&self) -> bool {
        matches!(
            self.recovery_strategy(),
            RecoveryStrategy::Retry | RecoveryStrategy::RetryWithBackoff
        )
    }
}

/// Resource allocation errors
#[derive(Debug, Clone)]
pub enum ResourceError {
    /// Insufficient resources
    Insufficient(String),

    /// Resource allocation failed
    AllocationFailed(String),

    /// Resource limit exceeded
    LimitExceeded(String),

    /// Quota exceeded
    QuotaExceeded(String),

    /// Invalid resource request
    InvalidRequest(String),

    /// Resource not found
    NotFound(String),

    /// Resource locked
    Locked(String),
}

impl ResourceError {
    /// Get error severity
    pub fn severity(&self) -> ErrorSeverity {
        match self {
            Self::Insufficient(_) => ErrorSeverity::Medium,
            Self::AllocationFailed(_) => ErrorSeverity::High,
            Self::LimitExceeded(_) => ErrorSeverity::Medium,
            Self::QuotaExceeded(_) => ErrorSeverity::Medium,
            Self::InvalidRequest(_) => ErrorSeverity::Low,
            Self::NotFound(_) => ErrorSeverity::Medium,
            Self::Locked(_) => ErrorSeverity::Medium,
        }
    }

    /// Get suggested recovery strategy
    pub fn recovery_strategy(&self) -> RecoveryStrategy {
        match self {
            Self::Insufficient(_) => RecoveryStrategy::RetryWithBackoff,
            Self::AllocationFailed(_) => RecoveryStrategy::Retry,
            Self::LimitExceeded(_) => RecoveryStrategy::Abort,
            Self::QuotaExceeded(_) => RecoveryStrategy::Abort,
            Self::InvalidRequest(_) => RecoveryStrategy::Abort,
            Self::NotFound(_) => RecoveryStrategy::Abort,
            Self::Locked(_) => RecoveryStrategy::RetryWithBackoff,
        }
    }

    /// Check if error is retryable
    pub fn is_retryable(&self) -> bool {
        matches!(
            self.recovery_strategy(),
            RecoveryStrategy::Retry | RecoveryStrategy::RetryWithBackoff
        )
    }
}

/// Integration errors
#[derive(Debug, Clone)]
pub enum IntegrationError {
    /// Workflow integration error
    Workflow(String),

    /// Scheduler integration error
    Scheduler(String),

    /// Storage integration error
    Storage(String),

    /// Security integration error
    Security(String),

    /// Monitoring integration error
    Monitoring(String),

    /// Communication error
    Communication(String),

    /// Serialization error
    Serialization(String),

    /// Generic integration error
    Other(String),
}

impl IntegrationError {
    /// Get error severity
    pub fn severity(&self) -> ErrorSeverity {
        match self {
            Self::Workflow(_) => ErrorSeverity::High,
            Self::Scheduler(_) => ErrorSeverity::High,
            Self::Storage(_) => ErrorSeverity::Critical,
            Self::Security(_) => ErrorSeverity::Critical,
            Self::Monitoring(_) => ErrorSeverity::Low,
            Self::Communication(_) => ErrorSeverity::High,
            Self::Serialization(_) => ErrorSeverity::Medium,
            Self::Other(_) => ErrorSeverity::Medium,
        }
    }

    /// Get suggested recovery strategy
    pub fn recovery_strategy(&self) -> RecoveryStrategy {
        match self {
            Self::Workflow(_) => RecoveryStrategy::Retry,
            Self::Scheduler(_) => RecoveryStrategy::Retry,
            Self::Storage(_) => RecoveryStrategy::Failover,
            Self::Security(_) => RecoveryStrategy::Abort,
            Self::Monitoring(_) => RecoveryStrategy::Ignore,
            Self::Communication(_) => RecoveryStrategy::Retry,
            Self::Serialization(_) => RecoveryStrategy::Abort,
            Self::Other(_) => RecoveryStrategy::Retry,
        }
    }

    /// Check if error is retryable
    pub fn is_retryable(&self) -> bool {
        matches!(
            self.recovery_strategy(),
            RecoveryStrategy::Retry | RecoveryStrategy::RetryWithBackoff
        )
    }
}

/// Event sourcing errors
#[derive(Debug, Clone)]
pub enum EventError {
    /// Event store operation failed
    StoreFailed(String),

    /// Event append failed
    AppendFailed(String),

    /// Event read failed
    ReadFailed(String),

    /// Event replay failed
    ReplayFailed(String),

    /// Event serialization failed
    SerializationFailed(String),

    /// Event deserialization failed
    DeserializationFailed(String),

    /// Invalid event sequence
    InvalidSequence(String),

    /// Event corruption
    Corruption(String),
}

impl EventError {
    /// Get error severity
    pub fn severity(&self) -> ErrorSeverity {
        match self {
            Self::StoreFailed(_) => ErrorSeverity::Critical,
            Self::AppendFailed(_) => ErrorSeverity::High,
            Self::ReadFailed(_) => ErrorSeverity::High,
            Self::ReplayFailed(_) => ErrorSeverity::Critical,
            Self::SerializationFailed(_) => ErrorSeverity::Medium,
            Self::DeserializationFailed(_) => ErrorSeverity::Medium,
            Self::InvalidSequence(_) => ErrorSeverity::High,
            Self::Corruption(_) => ErrorSeverity::Critical,
        }
    }

    /// Get suggested recovery strategy
    pub fn recovery_strategy(&self) -> RecoveryStrategy {
        match self {
            Self::StoreFailed(_) => RecoveryStrategy::Failover,
            Self::AppendFailed(_) => RecoveryStrategy::Retry,
            Self::ReadFailed(_) => RecoveryStrategy::Retry,
            Self::ReplayFailed(_) => RecoveryStrategy::Manual,
            Self::SerializationFailed(_) => RecoveryStrategy::Abort,
            Self::DeserializationFailed(_) => RecoveryStrategy::Abort,
            Self::InvalidSequence(_) => RecoveryStrategy::Manual,
            Self::Corruption(_) => RecoveryStrategy::Manual,
        }
    }

    /// Check if error is retryable
    pub fn is_retryable(&self) -> bool {
        matches!(
            self.recovery_strategy(),
            RecoveryStrategy::Retry | RecoveryStrategy::RetryWithBackoff
        )
    }
}

/// Storage errors
#[derive(Debug, Clone)]
pub enum StorageError {
    /// Storage operation failed
    OperationFailed(String),

    /// Lock-free queue error
    QueueError(String),

    /// Memory-mapped storage error
    MemoryMappedError(String),

    /// Object pool exhausted
    PoolExhausted(String),

    /// Ring buffer full
    RingBufferFull(String),

    /// Storage corruption
    Corruption(String),

    /// Storage unavailable
    Unavailable(String),
}

impl StorageError {
    /// Get error severity
    pub fn severity(&self) -> ErrorSeverity {
        match self {
            Self::OperationFailed(_) => ErrorSeverity::High,
            Self::QueueError(_) => ErrorSeverity::Medium,
            Self::MemoryMappedError(_) => ErrorSeverity::High,
            Self::PoolExhausted(_) => ErrorSeverity::Medium,
            Self::RingBufferFull(_) => ErrorSeverity::Medium,
            Self::Corruption(_) => ErrorSeverity::Critical,
            Self::Unavailable(_) => ErrorSeverity::High,
        }
    }

    /// Get suggested recovery strategy
    pub fn recovery_strategy(&self) -> RecoveryStrategy {
        match self {
            Self::OperationFailed(_) => RecoveryStrategy::Retry,
            Self::QueueError(_) => RecoveryStrategy::Retry,
            Self::MemoryMappedError(_) => RecoveryStrategy::Manual,
            Self::PoolExhausted(_) => RecoveryStrategy::RetryWithBackoff,
            Self::RingBufferFull(_) => RecoveryStrategy::RetryWithBackoff,
            Self::Corruption(_) => RecoveryStrategy::Manual,
            Self::Unavailable(_) => RecoveryStrategy::Failover,
        }
    }

    /// Check if error is retryable
    pub fn is_retryable(&self) -> bool {
        matches!(
            self.recovery_strategy(),
            RecoveryStrategy::Retry | RecoveryStrategy::RetryWithBackoff
        )
    }
}

// ================================================================================================
// ERROR AGGREGATION
// ================================================================================================

/// Aggregates multiple errors
#[derive(Debug, Clone)]
pub struct ErrorAggregate {
    /// List of errors
    pub errors: Vec<CoreError>,
    /// Error occurred at
    pub timestamp: Timestamptz,
}

impl ErrorAggregate {
    /// Create a new error aggregate
    pub fn new() -> Self {
        Self {
            errors: Vec::new(),
            timestamp: Timestamptz::now(),
        }
    }

    /// Add an error
    pub fn add(&mut self, error: CoreError) {
        self.errors.push(error);
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.errors.is_empty()
    }

    /// Get error count
    pub fn len(&self) -> usize {
        self.errors.len()
    }

    /// Get highest severity
    pub fn max_severity(&self) -> ErrorSeverity {
        self.errors
            .iter()
            .map(|e| e.severity())
            .max()
            .unwrap_or(ErrorSeverity::Low)
    }

    /// Check if any error is critical
    pub fn has_critical(&self) -> bool {
        self.max_severity() == ErrorSeverity::Critical
    }

    /// Convert to CoreError
    pub fn into_error(self) -> Option<CoreError> {
        if self.errors.is_empty() {
            None
        } else if self.errors.len() == 1 {
            self.errors.into_iter().next()
        } else {
            Some(CoreError::Internal(format!(
                "Multiple errors occurred: {}",
                self.errors
                    .iter()
                    .map(|e| e.to_string())
                    .collect::<Vec<_>>()
                    .join("; ")
            )))
        }
    }
}

impl Default for ErrorAggregate {
    fn default() -> Self {
        Self::new()
    }
}

// ================================================================================================
// CORE ERROR EXTENSIONS
// ================================================================================================

impl CoreError {
    /// Get error severity
    pub fn severity(&self) -> ErrorSeverity {
        match self {
            Self::Coordination(e) => e.severity(),
            Self::Worker(e) => e.severity(),
            Self::Task(e) => e.severity(),
            Self::Resource(e) => e.severity(),
            Self::Integration(e) => e.severity(),
            Self::Event(e) => e.severity(),
            Self::Storage(e) => e.severity(),
            Self::Configuration(_) => ErrorSeverity::High,
            Self::Timeout(_) => ErrorSeverity::Medium,
            Self::CircuitBreakerOpen(_) => ErrorSeverity::Medium,
            Self::NotFound(_) => ErrorSeverity::Low,
            Self::ValidationFailed(_) => ErrorSeverity::Medium,
            Self::NotImplemented(_) => ErrorSeverity::Low,
            Self::Internal(_) => ErrorSeverity::Critical,
        }
    }

    /// Get suggested recovery strategy
    pub fn recovery_strategy(&self) -> RecoveryStrategy {
        match self {
            Self::Coordination(e) => e.recovery_strategy(),
            Self::Worker(e) => e.recovery_strategy(),
            Self::Task(e) => e.recovery_strategy(),
            Self::Resource(e) => e.recovery_strategy(),
            Self::Integration(e) => e.recovery_strategy(),
            Self::Event(e) => e.recovery_strategy(),
            Self::Storage(e) => e.recovery_strategy(),
            Self::Configuration(_) => RecoveryStrategy::Abort,
            Self::Timeout(_) => RecoveryStrategy::RetryWithBackoff,
            Self::CircuitBreakerOpen(_) => RecoveryStrategy::RetryWithBackoff,
            Self::NotFound(_) => RecoveryStrategy::Abort,
            Self::ValidationFailed(_) => RecoveryStrategy::Abort,
            Self::NotImplemented(_) => RecoveryStrategy::Abort,
            Self::Internal(_) => RecoveryStrategy::Manual,
        }
    }

    /// Check if error is retryable
    pub fn is_retryable(&self) -> bool {
        match self {
            Self::Coordination(e) => e.is_retryable(),
            Self::Worker(e) => e.is_retryable(),
            Self::Task(e) => e.is_retryable(),
            Self::Resource(e) => e.is_retryable(),
            Self::Integration(e) => e.is_retryable(),
            Self::Event(e) => e.is_retryable(),
            Self::Storage(e) => e.is_retryable(),
            Self::Configuration(_) => false,
            Self::Timeout(_) => true,
            Self::CircuitBreakerOpen(_) => true,
            Self::NotFound(_) => false,
            Self::ValidationFailed(_) => false,
            Self::NotImplemented(_) => false,
            Self::Internal(_) => false,
        }
    }

    /// Create error context
    pub fn context(
        &self,
        component: impl Into<String>,
        operation: impl Into<String>,
    ) -> ErrorContext {
        ErrorContext::new(component, operation)
            .with_severity(self.severity())
            .with_recovery(self.recovery_strategy())
    }
}

// ================================================================================================
// DISPLAY IMPLEMENTATIONS
// ================================================================================================

impl fmt::Display for CoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Coordination(e) => write!(f, "Coordination error: {}", e),
            Self::Worker(e) => write!(f, "Worker error: {}", e),
            Self::Task(e) => write!(f, "Task error: {}", e),
            Self::Resource(e) => write!(f, "Resource error: {}", e),
            Self::Integration(e) => write!(f, "Integration error: {}", e),
            Self::Event(e) => write!(f, "Event error: {}", e),
            Self::Storage(e) => write!(f, "Storage error: {}", e),
            Self::Configuration(msg) => write!(f, "Configuration error: {}", msg),
            Self::Timeout(msg) => write!(f, "Timeout: {}", msg),
            Self::CircuitBreakerOpen(msg) => write!(f, "Circuit breaker open: {}", msg),
            Self::NotFound(msg) => write!(f, "Not found: {}", msg),
            Self::ValidationFailed(msg) => write!(f, "Validation failed: {}", msg),
            Self::NotImplemented(msg) => write!(f, "Not implemented: {}", msg),
            Self::Internal(msg) => write!(f, "Internal error: {}", msg),
        }
    }
}

impl fmt::Display for CoordinationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::SubmissionFailed(msg) => write!(f, "Task submission failed: {}", msg),
            Self::ProcessingFailed(msg) => write!(f, "Task processing failed: {}", msg),
            Self::DispatchFailed(msg) => write!(f, "Task dispatch failed: {}", msg),
            Self::InvalidState(msg) => write!(f, "Invalid coordination state: {}", msg),
            Self::Timeout(msg) => write!(f, "Coordination timeout: {}", msg),
            Self::QueueFull(msg) => write!(f, "Queue full: {}", msg),
            Self::Deadlock(msg) => write!(f, "Coordination deadlock: {}", msg),
            Self::StateConflict(msg) => write!(f, "State conflict: {}", msg),
        }
    }
}

impl fmt::Display for WorkerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotFound(msg) => write!(f, "Worker not found: {}", msg),
            Self::RegistrationFailed(msg) => write!(f, "Worker registration failed: {}", msg),
            Self::Unavailable(msg) => write!(f, "Worker unavailable: {}", msg),
            Self::HealthCheckFailed(msg) => write!(f, "Health check failed: {}", msg),
            Self::NoSuitableWorker(msg) => write!(f, "No suitable worker: {}", msg),
            Self::CapacityExceeded(msg) => write!(f, "Capacity exceeded: {}", msg),
            Self::CommunicationFailed(msg) => write!(f, "Communication failed: {}", msg),
            Self::PoolExhausted(msg) => write!(f, "Worker pool exhausted: {}", msg),
            Self::Disconnected(msg) => write!(f, "Worker disconnected: {}", msg),
        }
    }
}

impl fmt::Display for TaskError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotFound(msg) => write!(f, "Task not found: {}", msg),
            Self::ValidationFailed(msg) => write!(f, "Task validation failed: {}", msg),
            Self::InvalidState(msg) => write!(f, "Invalid task state: {}", msg),
            Self::AlreadyExists(msg) => write!(f, "Task already exists: {}", msg),
            Self::Timeout(msg) => write!(f, "Task timeout: {}", msg),
            Self::Cancelled(msg) => write!(f, "Task cancelled: {}", msg),
            Self::DependencyFailed(msg) => write!(f, "Task dependency failed: {}", msg),
            Self::ExecutionFailed(msg) => write!(f, "Task execution failed: {}", msg),
        }
    }
}

impl fmt::Display for ResourceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Insufficient(msg) => write!(f, "Insufficient resources: {}", msg),
            Self::AllocationFailed(msg) => write!(f, "Resource allocation failed: {}", msg),
            Self::LimitExceeded(msg) => write!(f, "Resource limit exceeded: {}", msg),
            Self::QuotaExceeded(msg) => write!(f, "Quota exceeded: {}", msg),
            Self::InvalidRequest(msg) => write!(f, "Invalid resource request: {}", msg),
            Self::NotFound(msg) => write!(f, "Resource not found: {}", msg),
            Self::Locked(msg) => write!(f, "Resource locked: {}", msg),
        }
    }
}

impl fmt::Display for IntegrationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Workflow(msg) => write!(f, "Workflow integration error: {}", msg),
            Self::Scheduler(msg) => write!(f, "Scheduler integration error: {}", msg),
            Self::Storage(msg) => write!(f, "Storage integration error: {}", msg),
            Self::Security(msg) => write!(f, "Security integration error: {}", msg),
            Self::Monitoring(msg) => write!(f, "Monitoring integration error: {}", msg),
            Self::Communication(msg) => write!(f, "Communication error: {}", msg),
            Self::Serialization(msg) => write!(f, "Serialization error: {}", msg),
            Self::Other(msg) => write!(f, "Integration error: {}", msg),
        }
    }
}

impl fmt::Display for EventError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::StoreFailed(msg) => write!(f, "Event store failed: {}", msg),
            Self::AppendFailed(msg) => write!(f, "Event append failed: {}", msg),
            Self::ReadFailed(msg) => write!(f, "Event read failed: {}", msg),
            Self::ReplayFailed(msg) => write!(f, "Event replay failed: {}", msg),
            Self::SerializationFailed(msg) => write!(f, "Event serialization failed: {}", msg),
            Self::DeserializationFailed(msg) => {
                write!(f, "Event deserialization failed: {}", msg)
            }
            Self::InvalidSequence(msg) => write!(f, "Invalid event sequence: {}", msg),
            Self::Corruption(msg) => write!(f, "Event corruption: {}", msg),
        }
    }
}

impl fmt::Display for StorageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::OperationFailed(msg) => write!(f, "Storage operation failed: {}", msg),
            Self::QueueError(msg) => write!(f, "Queue error: {}", msg),
            Self::MemoryMappedError(msg) => write!(f, "Memory-mapped error: {}", msg),
            Self::PoolExhausted(msg) => write!(f, "Object pool exhausted: {}", msg),
            Self::RingBufferFull(msg) => write!(f, "Ring buffer full: {}", msg),
            Self::Corruption(msg) => write!(f, "Storage corruption: {}", msg),
            Self::Unavailable(msg) => write!(f, "Storage unavailable: {}", msg),
        }
    }
}

// ================================================================================================
// ERROR TRAIT IMPLEMENTATIONS
// ================================================================================================

impl std::error::Error for CoreError {}
impl std::error::Error for CoordinationError {}
impl std::error::Error for WorkerError {}
impl std::error::Error for TaskError {}
impl std::error::Error for ResourceError {}
impl std::error::Error for IntegrationError {}
impl std::error::Error for EventError {}
impl std::error::Error for StorageError {}

// ================================================================================================
// CONVERSIONS FROM OTHER ERRORS
// ================================================================================================

impl From<OrcherError> for CoreError {
    fn from(err: OrcherError) -> Self {
        Self::Internal(err.to_string())
    }
}

impl From<orcher_config::error::ConfigError> for CoreError {
    fn from(err: orcher_config::error::ConfigError) -> Self {
        Self::Configuration(err.to_string())
    }
}

impl From<orcher_events::error::EventError> for CoreError {
    fn from(err: orcher_events::error::EventError) -> Self {
        Self::Event(EventError::StoreFailed(err.to_string()))
    }
}

// ================================================================================================
// HELPER CONSTRUCTORS
// ================================================================================================

impl CoreError {
    /// Create a coordination error
    pub fn coordination(msg: impl Into<String>) -> Self {
        Self::Coordination(CoordinationError::SubmissionFailed(msg.into()))
    }

    /// Create a worker error
    pub fn worker(msg: impl Into<String>) -> Self {
        Self::Worker(WorkerError::NotFound(msg.into()))
    }

    /// Create a task error
    pub fn task(msg: impl Into<String>) -> Self {
        Self::Task(TaskError::NotFound(msg.into()))
    }

    /// Create a resource error
    pub fn resource(msg: impl Into<String>) -> Self {
        Self::Resource(ResourceError::Insufficient(msg.into()))
    }

    /// Create an integration error
    pub fn integration(msg: impl Into<String>) -> Self {
        Self::Integration(IntegrationError::Other(msg.into()))
    }

    /// Create an internal error
    pub fn internal(msg: impl Into<String>) -> Self {
        Self::Internal(msg.into())
    }

    /// Create a queue error
    pub fn queue(msg: impl Into<String>) -> Self {
        Self::Storage(StorageError::QueueError(msg.into()))
    }

    /// Create a connection error
    pub fn connection(msg: impl Into<String>) -> Self {
        Self::Integration(IntegrationError::Communication(msg.into()))
    }

    /// Create a serialization error
    pub fn serialization(msg: impl Into<String>) -> Self {
        Self::Integration(IntegrationError::Serialization(msg.into()))
    }

    /// Create a deserialization error
    pub fn deserialization(msg: impl Into<String>) -> Self {
        Self::Integration(IntegrationError::Serialization(msg.into()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_creation() {
        let err = CoreError::coordination("test");
        assert!(matches!(err, CoreError::Coordination(_)));
    }

    #[test]
    fn test_error_display() {
        let err = CoreError::worker("worker-1");
        let display = format!("{}", err);
        assert!(display.contains("Worker error"));
    }

    #[test]
    fn test_error_conversion() {
        let orcher_err = OrcherError::internal("test");
        let core_err: CoreError = orcher_err.into();
        assert!(matches!(core_err, CoreError::Internal(_)));
    }

    #[test]
    fn test_error_severity() {
        let err = CoreError::Coordination(CoordinationError::Deadlock("test".to_string()));
        assert_eq!(err.severity(), ErrorSeverity::Critical);

        let err2 = CoreError::Task(TaskError::ValidationFailed("test".to_string()));
        assert_eq!(err2.severity(), ErrorSeverity::Low);
    }

    #[test]
    fn test_error_recovery_strategy() {
        let err = CoreError::Worker(WorkerError::Unavailable("test".to_string()));
        assert_eq!(err.recovery_strategy(), RecoveryStrategy::Failover);

        let err2 = CoreError::Resource(ResourceError::Insufficient("test".to_string()));
        assert_eq!(err2.recovery_strategy(), RecoveryStrategy::RetryWithBackoff);
    }

    #[test]
    fn test_error_retryable() {
        let err = CoreError::Coordination(CoordinationError::SubmissionFailed("test".to_string()));
        assert!(err.is_retryable());

        let err2 = CoreError::Task(TaskError::ValidationFailed("test".to_string()));
        assert!(!err2.is_retryable());

        let err3 = CoreError::Worker(WorkerError::CommunicationFailed("test".to_string()));
        assert!(err3.is_retryable());
    }

    #[test]
    fn test_error_context() {
        let err = CoreError::coordination("test");
        let ctx = err.context("coordinator", "submit_task");
        assert_eq!(ctx.component, "coordinator");
        assert_eq!(ctx.operation, "submit_task");
        assert_eq!(ctx.severity, ErrorSeverity::High);
    }

    #[test]
    fn test_error_context_builder() {
        let ctx = ErrorContext::new("test_component", "test_operation")
            .with_context("key1", "value1")
            .with_severity(ErrorSeverity::Critical)
            .with_recovery(RecoveryStrategy::Retry);

        assert_eq!(ctx.component, "test_component");
        assert_eq!(ctx.severity, ErrorSeverity::Critical);
        assert_eq!(ctx.recovery, RecoveryStrategy::Retry);
        assert_eq!(ctx.context.get("key1"), Some(&"value1".to_string()));
    }

    #[test]
    fn test_error_aggregate() {
        let mut aggregate = ErrorAggregate::new();
        assert!(aggregate.is_empty());
        assert_eq!(aggregate.len(), 0);

        aggregate.add(CoreError::coordination("test1"));
        aggregate.add(CoreError::worker("test2"));
        assert_eq!(aggregate.len(), 2);
        assert!(!aggregate.is_empty());
    }

    #[test]
    fn test_error_aggregate_severity() {
        let mut aggregate = ErrorAggregate::new();
        aggregate.add(CoreError::Task(TaskError::ValidationFailed(
            "test".to_string(),
        )));
        aggregate.add(CoreError::Coordination(CoordinationError::Deadlock(
            "test".to_string(),
        )));

        assert_eq!(aggregate.max_severity(), ErrorSeverity::Critical);
        assert!(aggregate.has_critical());
    }

    #[test]
    fn test_error_aggregate_into_error() {
        let mut aggregate = ErrorAggregate::new();
        assert!(aggregate.clone().into_error().is_none());

        aggregate.add(CoreError::coordination("test"));
        let result = aggregate.into_error();
        assert!(result.is_some());
    }

    #[test]
    fn test_coordination_error_methods() {
        let err = CoordinationError::SubmissionFailed("test".to_string());
        assert_eq!(err.severity(), ErrorSeverity::High);
        assert_eq!(err.recovery_strategy(), RecoveryStrategy::Retry);
        assert!(err.is_retryable());

        let err2 = CoordinationError::Deadlock("test".to_string());
        assert_eq!(err2.severity(), ErrorSeverity::Critical);
        assert_eq!(err2.recovery_strategy(), RecoveryStrategy::Manual);
        assert!(!err2.is_retryable());
    }

    #[test]
    fn test_worker_error_methods() {
        let err = WorkerError::PoolExhausted("test".to_string());
        assert_eq!(err.severity(), ErrorSeverity::Critical);
        assert_eq!(err.recovery_strategy(), RecoveryStrategy::Manual);
        assert!(!err.is_retryable());

        let err2 = WorkerError::Unavailable("test".to_string());
        assert_eq!(err2.severity(), ErrorSeverity::Medium);
        assert_eq!(err2.recovery_strategy(), RecoveryStrategy::Failover);
        assert!(!err2.is_retryable());
    }

    #[test]
    fn test_task_error_methods() {
        let err = TaskError::DependencyFailed("test".to_string());
        assert_eq!(err.severity(), ErrorSeverity::High);
        assert_eq!(err.recovery_strategy(), RecoveryStrategy::Abort);
        assert!(!err.is_retryable());

        let err2 = TaskError::ExecutionFailed("test".to_string());
        assert_eq!(err2.severity(), ErrorSeverity::High);
        assert_eq!(err2.recovery_strategy(), RecoveryStrategy::Retry);
        assert!(err2.is_retryable());
    }

    #[test]
    fn test_resource_error_methods() {
        let err = ResourceError::Insufficient("test".to_string());
        assert_eq!(err.severity(), ErrorSeverity::Medium);
        assert_eq!(err.recovery_strategy(), RecoveryStrategy::RetryWithBackoff);
        assert!(err.is_retryable());

        let err2 = ResourceError::Locked("test".to_string());
        assert_eq!(err2.severity(), ErrorSeverity::Medium);
        assert_eq!(err2.recovery_strategy(), RecoveryStrategy::RetryWithBackoff);
        assert!(err2.is_retryable());
    }

    #[test]
    fn test_integration_error_methods() {
        let err = IntegrationError::Storage("test".to_string());
        assert_eq!(err.severity(), ErrorSeverity::Critical);
        assert_eq!(err.recovery_strategy(), RecoveryStrategy::Failover);
        assert!(!err.is_retryable());

        let err2 = IntegrationError::Communication("test".to_string());
        assert_eq!(err2.severity(), ErrorSeverity::High);
        assert_eq!(err2.recovery_strategy(), RecoveryStrategy::Retry);
        assert!(err2.is_retryable());
    }

    #[test]
    fn test_event_error_methods() {
        let err = EventError::StoreFailed("test".to_string());
        assert_eq!(err.severity(), ErrorSeverity::Critical);
        assert_eq!(err.recovery_strategy(), RecoveryStrategy::Failover);
        assert!(!err.is_retryable());

        let err2 = EventError::AppendFailed("test".to_string());
        assert_eq!(err2.severity(), ErrorSeverity::High);
        assert_eq!(err2.recovery_strategy(), RecoveryStrategy::Retry);
        assert!(err2.is_retryable());
    }

    #[test]
    fn test_storage_error_methods() {
        let err = StorageError::Corruption("test".to_string());
        assert_eq!(err.severity(), ErrorSeverity::Critical);
        assert_eq!(err.recovery_strategy(), RecoveryStrategy::Manual);
        assert!(!err.is_retryable());

        let err2 = StorageError::RingBufferFull("test".to_string());
        assert_eq!(err2.severity(), ErrorSeverity::Medium);
        assert_eq!(err2.recovery_strategy(), RecoveryStrategy::RetryWithBackoff);
        assert!(err2.is_retryable());
    }

    #[test]
    fn test_error_severity_ordering() {
        assert!(ErrorSeverity::Low < ErrorSeverity::Medium);
        assert!(ErrorSeverity::Medium < ErrorSeverity::High);
        assert!(ErrorSeverity::High < ErrorSeverity::Critical);
    }

    #[test]
    fn test_new_error_variants() {
        // Test new coordination errors
        let err = CoordinationError::StateConflict("test".to_string());
        assert!(format!("{}", err).contains("State conflict"));

        // Test new worker errors
        let err = WorkerError::Disconnected("test".to_string());
        assert!(format!("{}", err).contains("disconnected"));

        // Test new task errors
        let err = TaskError::DependencyFailed("test".to_string());
        assert!(format!("{}", err).contains("dependency failed"));

        // Test new resource errors
        let err = ResourceError::Locked("test".to_string());
        assert!(format!("{}", err).contains("locked"));

        // Test new integration errors
        let err = IntegrationError::Serialization("test".to_string());
        assert!(format!("{}", err).contains("Serialization"));

        // Test new event error
        let err = EventError::Corruption("test".to_string());
        assert!(format!("{}", err).contains("corruption"));

        // Test new storage errors
        let err = StorageError::Unavailable("test".to_string());
        assert!(format!("{}", err).contains("unavailable"));
    }
}
