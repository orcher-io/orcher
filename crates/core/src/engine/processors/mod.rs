//! Background queue processors (outbox, timers, schedules, batch operations).

pub mod batch_processor;
pub mod outbox_processor;
pub mod schedule_processor;
pub mod timer_processor;

// Re-export commonly used types
pub use batch_processor::{BatchMetrics, BatchProcessor, BatchProcessorConfig};
pub use outbox_processor::{
    OutboxProcessor, OutboxProcessorConfig, PartitionedOutboxConfig, PartitionedOutboxProcessor,
    PartitionedProcessorMetrics, ProcessorMetrics,
};
pub use schedule_processor::{ScheduleMetrics, ScheduleProcessor, ScheduleProcessorConfig};
pub use timer_processor::{TimerMetrics, TimerProcessor, TimerProcessorConfig};
