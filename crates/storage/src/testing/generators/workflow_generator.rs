//! # Workflow Test Data Generator
//!
//! This module provides utilities for generating workflow test data,
//! using orcher-common types and test utilities.

use orcher_common::testing::TestDataGenerator;
use orcher_common::types::*;
use std::collections::HashMap;

/// Generate a random workflow with default values
pub fn generate_random_workflow() -> HashMap<String, String> {
    let mut data = HashMap::new();
    data.insert(
        "workflow_id".to_string(),
        TestDataGenerator::random_workflow_id().to_string(),
    );
    data.insert(
        "name".to_string(),
        format!("workflow-{}", TestDataGenerator::random_string(8)),
    );
    data.insert("status".to_string(), "pending".to_string());
    data
}

/// Generate a workflow with specific status
pub fn generate_workflow_with_status(status: &str) -> HashMap<String, String> {
    let mut data = generate_random_workflow();
    data.insert("status".to_string(), status.to_string());
    data
}

/// Generate a workflow with specific ID
pub fn generate_workflow_with_id(workflow_id: WorkflowId) -> HashMap<String, String> {
    let mut data = generate_random_workflow();
    data.insert("workflow_id".to_string(), workflow_id.to_string());
    data
}

/// Generate multiple workflows
pub fn generate_workflows(count: usize) -> Vec<HashMap<String, String>> {
    (0..count).map(|_| generate_random_workflow()).collect()
}

/// Generate a pending workflow
pub fn generate_pending_workflow() -> HashMap<String, String> {
    generate_workflow_with_status("pending")
}

/// Generate a running workflow
pub fn generate_running_workflow() -> HashMap<String, String> {
    generate_workflow_with_status("running")
}

/// Generate a completed workflow
pub fn generate_completed_workflow() -> HashMap<String, String> {
    generate_workflow_with_status("completed")
}

/// Generate a failed workflow
pub fn generate_failed_workflow() -> HashMap<String, String> {
    generate_workflow_with_status("failed")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_random_workflow() {
        let workflow = generate_random_workflow();
        assert!(workflow.contains_key("workflow_id"));
        assert!(workflow.contains_key("name"));
        assert!(workflow.contains_key("status"));
        assert_eq!(workflow.get("status"), Some(&"pending".to_string()));
    }

    #[test]
    fn test_generate_workflow_with_status() {
        let workflow = generate_workflow_with_status("running");
        assert_eq!(workflow.get("status"), Some(&"running".to_string()));
    }

    #[test]
    fn test_generate_workflow_with_id() {
        let workflow_id = WorkflowId::new_v4();
        let workflow = generate_workflow_with_id(workflow_id);
        assert_eq!(workflow.get("workflow_id"), Some(&workflow_id.to_string()));
    }

    #[test]
    fn test_generate_workflows() {
        let workflows = generate_workflows(3);
        assert_eq!(workflows.len(), 3);
        for workflow in workflows {
            assert!(workflow.contains_key("workflow_id"));
        }
    }

    #[test]
    fn test_generate_pending_workflow() {
        let workflow = generate_pending_workflow();
        assert_eq!(workflow.get("status"), Some(&"pending".to_string()));
    }

    #[test]
    fn test_generate_running_workflow() {
        let workflow = generate_running_workflow();
        assert_eq!(workflow.get("status"), Some(&"running".to_string()));
    }

    #[test]
    fn test_generate_completed_workflow() {
        let workflow = generate_completed_workflow();
        assert_eq!(workflow.get("status"), Some(&"completed".to_string()));
    }

    #[test]
    fn test_generate_failed_workflow() {
        let workflow = generate_failed_workflow();
        assert_eq!(workflow.get("status"), Some(&"failed".to_string()));
    }
}
