//! Retry and timeout infrastructure.

/// Task retry policy evaluation
pub mod retry_policy;

/// Task retry scheduler
pub mod retry_scheduler;

/// Background dispatcher for ready task retries
pub mod retry_dispatcher;

/// Ending the queued tasks a workflow's cancellation interrupted
pub mod cancelled_before_start;

pub(crate) mod task_failure;

/// Task timeout watcher
pub mod timeout_watcher;

// Re-exports for convenience
pub use cancelled_before_start::QueuedTaskCancellation;
pub use retry_dispatcher::{TaskRetryDispatcher, DEFAULT_SWEEP_INTERVAL};
pub use retry_policy::{DlqReason, RetryDecision, RetryPolicy, RetryPolicyEvaluator};
pub use retry_scheduler::{RetryOutcome, RetryStats, TaskRetryScheduler};
pub use timeout_watcher::{TimeoutStatistics, TimeoutWatcher, TimeoutWatcherConfig};
