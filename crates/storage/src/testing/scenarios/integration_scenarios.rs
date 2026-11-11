//! # Integration Test Scenarios
//!
//! This module provides integration test scenarios for validating
//! storage operations with orcher-common types and cross-crate compatibility.

#[cfg(test)]
use crate::testing::generators;
#[cfg(test)]
use orcher_common::types::*;

/// Scenario: Create and retrieve a task
#[cfg(test)]
pub fn scenario_create_and_retrieve_task() -> (TaskId, std::collections::HashMap<String, String>) {
    let task = generators::generate_random_task();
    let task_id = task
        .get("task_id")
        .and_then(|id| id.parse().ok())
        .unwrap_or_else(TaskId::new_v4);
    (task_id, task)
}

/// Scenario: Create multiple tasks with different statuses
#[cfg(test)]
pub fn scenario_multiple_tasks_with_statuses() -> Vec<std::collections::HashMap<String, String>> {
    vec![
        generators::generate_pending_task(),
        generators::generate_running_task(),
        generators::generate_completed_task(),
        generators::generate_failed_task(),
    ]
}

/// Scenario: Workflow with associated tasks
#[cfg(test)]
pub fn scenario_workflow_with_tasks(
    task_count: usize,
) -> (
    WorkflowId,
    std::collections::HashMap<String, String>,
    Vec<std::collections::HashMap<String, String>>,
) {
    let workflow = generators::generate_random_workflow();
    let workflow_id = workflow
        .get("workflow_id")
        .and_then(|id| id.parse().ok())
        .unwrap_or_else(WorkflowId::new_v4);

    let tasks = generators::generate_tasks(task_count);
    (workflow_id, workflow, tasks)
}

/// Scenario: Task state transitions
#[cfg(test)]
pub fn scenario_task_state_transitions() -> Vec<&'static str> {
    vec!["pending", "running", "completed"]
}

/// Scenario: Workflow state transitions
#[cfg(test)]
pub fn scenario_workflow_state_transitions() -> Vec<&'static str> {
    vec!["pending", "running", "completed", "failed"]
}

/// Scenario: Batch task creation
#[cfg(test)]
pub fn scenario_batch_task_creation(
    count: usize,
) -> Vec<std::collections::HashMap<String, String>> {
    generators::generate_tasks(count)
}

/// Scenario: Batch workflow creation
#[cfg(test)]
pub fn scenario_batch_workflow_creation(
    count: usize,
) -> Vec<std::collections::HashMap<String, String>> {
    generators::generate_workflows(count)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_scenario_create_and_retrieve_task() {
        let (task_id, task) = scenario_create_and_retrieve_task();
        assert!(!task_id.to_string().is_empty());
        assert!(task.contains_key("task_id"));
    }

    #[test]
    fn test_scenario_multiple_tasks_with_statuses() {
        let tasks = scenario_multiple_tasks_with_statuses();
        assert_eq!(tasks.len(), 4);
        assert_eq!(tasks[0].get("status"), Some(&"pending".to_string()));
        assert_eq!(tasks[1].get("status"), Some(&"running".to_string()));
        assert_eq!(tasks[2].get("status"), Some(&"completed".to_string()));
        assert_eq!(tasks[3].get("status"), Some(&"failed".to_string()));
    }

    #[test]
    fn test_scenario_workflow_with_tasks() {
        let (workflow_id, workflow, tasks) = scenario_workflow_with_tasks(3);
        assert!(!workflow_id.to_string().is_empty());
        assert!(workflow.contains_key("workflow_id"));
        assert_eq!(tasks.len(), 3);
    }

    #[test]
    fn test_scenario_task_state_transitions() {
        let states = scenario_task_state_transitions();
        assert_eq!(states.len(), 3);
        assert_eq!(states[0], "pending");
        assert_eq!(states[1], "running");
        assert_eq!(states[2], "completed");
    }

    #[test]
    fn test_scenario_workflow_state_transitions() {
        let states = scenario_workflow_state_transitions();
        assert_eq!(states.len(), 4);
        assert!(states.contains(&"pending"));
        assert!(states.contains(&"failed"));
    }

    #[test]
    fn test_scenario_batch_task_creation() {
        let tasks = scenario_batch_task_creation(5);
        assert_eq!(tasks.len(), 5);
        for task in tasks {
            assert!(task.contains_key("task_id"));
        }
    }

    #[test]
    fn test_scenario_batch_workflow_creation() {
        let workflows = scenario_batch_workflow_creation(3);
        assert_eq!(workflows.len(), 3);
        for workflow in workflows {
            assert!(workflow.contains_key("workflow_id"));
        }
    }
}
