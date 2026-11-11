//! Domain event types — one enum per aggregate (task, workflow, worker, etc.).

pub mod dlq;
pub mod execution;
pub mod scheduler;
pub mod task;
pub mod worker;
pub mod worker_task;
pub mod workflow;

pub use dlq::{AlertSeverity, DlqEvent, FailureType};
pub use execution::ExecutionEvent;
pub use scheduler::SchedulerEvent;
pub use task::{TaskEvent, TaskResult};
pub use worker::WorkerEvent;
pub use worker_task::{WorkerTaskEvent, WorkerTaskEventType};
pub use workflow::WorkflowEvent;

#[cfg(test)]
mod tests {

    #[test]
    fn test_module_exports() {
        // Verify that DlqEvent is accessible
        use crate::domain::DlqEvent;
        let _ = std::any::type_name::<DlqEvent>();

        // Verify that TaskEvent is accessible
        use crate::domain::TaskEvent;
        let _ = std::any::type_name::<TaskEvent>();

        // Verify that WorkflowEvent is accessible
        use crate::domain::WorkflowEvent;
        let _ = std::any::type_name::<WorkflowEvent>();

        // Verify that WorkerEvent is accessible
        use crate::domain::WorkerEvent;
        let _ = std::any::type_name::<WorkerEvent>();

        // Verify that WorkerTaskEvent is accessible
        use crate::domain::WorkerTaskEvent;
        let _ = std::any::type_name::<WorkerTaskEvent>();

        // Verify that ExecutionEvent is accessible
        use crate::domain::ExecutionEvent;
        let _ = std::any::type_name::<ExecutionEvent>();

        // Verify that SchedulerEvent is accessible
        use crate::domain::SchedulerEvent;
        let _ = std::any::type_name::<SchedulerEvent>();
    }
}
