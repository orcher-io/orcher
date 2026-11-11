//! # Workflow Test Fixtures
//!
//! This module provides test fixtures for workflow-related testing,
//! using orcher-common types and test utilities.

use orcher_common::testing::TestFixture;
use orcher_common::types::*;

/// Create a basic workflow fixture
pub fn basic_workflow_fixture() -> TestFixture {
    TestFixture::workflow()
}

/// Create a workflow fixture with custom ID
pub fn workflow_fixture_with_id(workflow_id: WorkflowId) -> TestFixture {
    let mut fixture = TestFixture::workflow();
    fixture
        .data
        .insert("workflow_id".to_string(), workflow_id.to_string());
    fixture
}

/// Create a running workflow fixture
pub fn running_workflow_fixture() -> TestFixture {
    let mut fixture = TestFixture::workflow();
    fixture
        .data
        .insert("status".to_string(), "running".to_string());
    fixture
}

/// Create a completed workflow fixture
pub fn completed_workflow_fixture() -> TestFixture {
    let mut fixture = TestFixture::workflow();
    fixture
        .data
        .insert("status".to_string(), "completed".to_string());
    fixture
}

/// Create a failed workflow fixture
pub fn failed_workflow_fixture() -> TestFixture {
    let mut fixture = TestFixture::workflow();
    fixture
        .data
        .insert("status".to_string(), "failed".to_string());
    fixture
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_workflow_fixture() {
        let fixture = basic_workflow_fixture();
        assert_eq!(fixture.name, "workflow");
        assert!(fixture.get_data("workflow_id").is_some());
    }

    #[test]
    fn test_workflow_fixture_with_id() {
        let workflow_id = WorkflowId::new_v4();
        let fixture = workflow_fixture_with_id(workflow_id);
        assert!(fixture.get_data("workflow_id").is_some());
    }

    #[test]
    fn test_running_workflow_fixture() {
        let fixture = running_workflow_fixture();
        assert_eq!(fixture.get_data("status"), Some(&"running".to_string()));
    }

    #[test]
    fn test_completed_workflow_fixture() {
        let fixture = completed_workflow_fixture();
        assert_eq!(fixture.get_data("status"), Some(&"completed".to_string()));
    }

    #[test]
    fn test_failed_workflow_fixture() {
        let fixture = failed_workflow_fixture();
        assert_eq!(fixture.get_data("status"), Some(&"failed".to_string()));
    }
}
