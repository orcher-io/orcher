//! # Data Assertions for Storage Testing
//!
//! This module provides custom assertion utilities for validating
//! storage data integrity and consistency in tests.

use orcher_common::types::*;

/// Assert that a task ID is valid
pub fn assert_valid_task_id(task_id: &TaskId) {
    assert!(
        !task_id.to_string().is_empty(),
        "Task ID should not be empty"
    );
}

/// Assert that a workflow ID is valid
pub fn assert_valid_workflow_id(workflow_id: &WorkflowId) {
    assert!(
        !workflow_id.to_string().is_empty(),
        "Workflow ID should not be empty"
    );
}

/// Assert that a user ID is valid
pub fn assert_valid_user_id(user_id: &UserId) {
    assert!(
        !user_id.to_string().is_empty(),
        "User ID should not be empty"
    );
}

/// Assert that two task IDs are equal
pub fn assert_task_ids_equal(expected: &TaskId, actual: &TaskId) {
    assert_eq!(
        expected, actual,
        "Task IDs should be equal: expected={}, actual={}",
        expected, actual
    );
}

/// Assert that two workflow IDs are equal
pub fn assert_workflow_ids_equal(expected: &WorkflowId, actual: &WorkflowId) {
    assert_eq!(
        expected, actual,
        "Workflow IDs should be equal: expected={}, actual={}",
        expected, actual
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_assert_valid_task_id() {
        let task_id = TaskId::new_v4();
        assert_valid_task_id(&task_id);
    }

    #[test]
    fn test_assert_valid_workflow_id() {
        let workflow_id = WorkflowId::new_v4();
        assert_valid_workflow_id(&workflow_id);
    }

    #[test]
    fn test_assert_valid_user_id() {
        let user_id = UserId::new_v4();
        assert_valid_user_id(&user_id);
    }

    #[test]
    fn test_assert_task_ids_equal() {
        let task_id = TaskId::new_v4();
        assert_task_ids_equal(&task_id, &task_id);
    }

    #[test]
    fn test_assert_workflow_ids_equal() {
        let workflow_id = WorkflowId::new_v4();
        assert_workflow_ids_equal(&workflow_id, &workflow_id);
    }
}
