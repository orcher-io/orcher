//! # Task Test Data Generator
//!
//! This module provides utilities for generating task test data,
//! using orcher-common types and test utilities.

use orcher_common::testing::TestDataGenerator;
use orcher_common::types::*;
use std::collections::HashMap;

/// Generate a random task with default values
pub fn generate_random_task() -> HashMap<String, String> {
    let mut data = HashMap::new();
    data.insert(
        "task_id".to_string(),
        TestDataGenerator::random_task_id().to_string(),
    );
    data.insert(
        "name".to_string(),
        format!("task-{}", TestDataGenerator::random_string(8)),
    );
    data.insert("status".to_string(), "pending".to_string());
    data.insert("priority".to_string(), "1".to_string());
    data
}

/// Generate a task with specific status
pub fn generate_task_with_status(status: &str) -> HashMap<String, String> {
    let mut data = generate_random_task();
    data.insert("status".to_string(), status.to_string());
    data
}

/// Generate a task with specific ID
pub fn generate_task_with_id(task_id: TaskId) -> HashMap<String, String> {
    let mut data = generate_random_task();
    data.insert("task_id".to_string(), task_id.to_string());
    data
}

/// Generate multiple tasks
pub fn generate_tasks(count: usize) -> Vec<HashMap<String, String>> {
    (0..count).map(|_| generate_random_task()).collect()
}

/// Generate a pending task
pub fn generate_pending_task() -> HashMap<String, String> {
    generate_task_with_status("pending")
}

/// Generate a running task
pub fn generate_running_task() -> HashMap<String, String> {
    generate_task_with_status("running")
}

/// Generate a completed task
pub fn generate_completed_task() -> HashMap<String, String> {
    generate_task_with_status("completed")
}

/// Generate a failed task
pub fn generate_failed_task() -> HashMap<String, String> {
    generate_task_with_status("failed")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_random_task() {
        let task = generate_random_task();
        assert!(task.contains_key("task_id"));
        assert!(task.contains_key("name"));
        assert!(task.contains_key("status"));
        assert_eq!(task.get("status"), Some(&"pending".to_string()));
    }

    #[test]
    fn test_generate_task_with_status() {
        let task = generate_task_with_status("running");
        assert_eq!(task.get("status"), Some(&"running".to_string()));
    }

    #[test]
    fn test_generate_task_with_id() {
        let task_id = TaskId::new_v4();
        let task = generate_task_with_id(task_id);
        assert_eq!(task.get("task_id"), Some(&task_id.to_string()));
    }

    #[test]
    fn test_generate_tasks() {
        let tasks = generate_tasks(5);
        assert_eq!(tasks.len(), 5);
        for task in tasks {
            assert!(task.contains_key("task_id"));
        }
    }

    #[test]
    fn test_generate_pending_task() {
        let task = generate_pending_task();
        assert_eq!(task.get("status"), Some(&"pending".to_string()));
    }

    #[test]
    fn test_generate_running_task() {
        let task = generate_running_task();
        assert_eq!(task.get("status"), Some(&"running".to_string()));
    }

    #[test]
    fn test_generate_completed_task() {
        let task = generate_completed_task();
        assert_eq!(task.get("status"), Some(&"completed".to_string()));
    }

    #[test]
    fn test_generate_failed_task() {
        let task = generate_failed_task();
        assert_eq!(task.get("status"), Some(&"failed".to_string()));
    }
}
