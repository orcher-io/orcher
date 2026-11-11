//! Queue entry types.

#![allow(missing_docs)]

// Re-export all coordination types from storage
pub use orcher_storage::schema::types::coordination::{
    DeadLetterTask, OutboxTask, OutboxTaskType, QueueMetrics, TaskStatus, TimerTask, TimerTaskType,
};

// Legacy aliases for backward compatibility
#[deprecated(since = "0.2.0", note = "Use OutboxTask from orcher_storage instead")]
pub type OutboxQueueTask = OutboxTask;

#[deprecated(since = "0.2.0", note = "Use TimerTask from orcher_storage instead")]
pub type TimerQueueTask = TimerTask;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_types_are_exported() {
        // Compile-time test to ensure all types are properly re-exported
        fn _assert_types_available() {
            let _: OutboxTask;
            let _: TimerTask;
            let _: DeadLetterTask;
            let _: TaskStatus;
            let _: OutboxTaskType;
            let _: TimerTaskType;
            let _: QueueMetrics;
        }
    }
}
