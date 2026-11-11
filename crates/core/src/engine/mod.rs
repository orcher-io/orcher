//! Coordination engine (outbox, timers, dispatch, state management).

pub mod batching;
pub mod coordinator;
pub mod cron;
pub mod dispatch;
pub mod metrics;
pub mod processors;
pub mod queues;
pub mod state;
pub mod system;
pub mod timers;

// Re-export commonly used types
pub use coordinator::{
    undispatched_repair_window, CoordinatorConfig, CoordinatorMetrics, TaskDispatch,
    WorkflowCoordinator,
};
pub use metrics::{
    generate_grafana_dashboard, metrics_endpoint_handler, metrics_endpoint_handler_with_labels,
    PrometheusExporter,
};
pub use processors::{
    OutboxProcessor, OutboxProcessorConfig, ProcessorMetrics, ScheduleMetrics, ScheduleProcessor,
    ScheduleProcessorConfig, TimerMetrics, TimerProcessor, TimerProcessorConfig,
};
pub use queues::{
    DeadLetterTask, OutboxQueue, OutboxTask, OutboxTaskType, QueueMetrics, ScheduleQueue,
    TaskStatus, TimerQueue, TimerTask, TimerTaskType,
};
pub use state::{
    CacheConfig, CacheInvalidator, CacheStats, EventGateInfo, EventType, ExecutionEvent,
    InvalidationMessage, InvalidationMetrics, InvalidationType, InvalidatorConfig, LockConfig,
    LockGuard, StateReconstructor, TaskInfo, TimerInfo, WorkflowExecutionState, WorkflowStateCache,
    WorkflowStateLock, WorkflowStatus,
};
pub use system::{
    OrchestrationConfig, OrchestrationSystem, OrchestrationSystemBuilder, ProcessorState,
    SystemHealth, SystemSummary,
};
pub use timers::{
    AcceleratorConfig, AcceleratorStats, AddTimerResult, ExternalTimer, LoadResult,
    TimerAccelerator, TimerEntry, TimerId, TimerWheel, TimerWheelConfig, TimerWheelMetrics,
    TimerWheelStats,
};
