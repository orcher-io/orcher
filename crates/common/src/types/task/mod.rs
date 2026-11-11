//! Task status and priority types.

pub mod priority;
pub mod status;

pub use priority::{TaskPriority, TaskPriorityLevel};
pub use status::TaskStatus;

pub use crate::types::identifiers::{TaskExecutionId, TaskId};

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::identifiers::traits::Identifier;

    #[test]
    fn test_task_module_imports() {
        let task_id = TaskId::generate();
        assert!(task_id.is_valid());

        let priority = TaskPriority::default();
        assert_eq!(priority.level(), TaskPriorityLevel::Normal);

        let status = TaskStatus::default();
        assert_eq!(status, TaskStatus::Pending);
    }
}
