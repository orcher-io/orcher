//! # Task Test Fixtures
//!
//! This module provides test fixtures for task-related testing,
//! using orcher-common types and test utilities.

use orcher_common::testing::TestFixture;
use orcher_common::types::*;

/// Create a basic task fixture
pub fn basic_task_fixture() -> TestFixture {
    TestFixture::task()
}

/// Create a task fixture with custom ID
pub fn task_fixture_with_id(task_id: TaskId) -> TestFixture {
    let mut fixture = TestFixture::task();
    fixture
        .data
        .insert("task_id".to_string(), task_id.to_string());
    fixture
}

/// Create a pending task fixture
pub fn pending_task_fixture() -> TestFixture {
    let mut fixture = TestFixture::task();
    fixture
        .data
        .insert("status".to_string(), "pending".to_string());
    fixture
}

/// Create a running task fixture
pub fn running_task_fixture() -> TestFixture {
    let mut fixture = TestFixture::task();
    fixture
        .data
        .insert("status".to_string(), "running".to_string());
    fixture
}

/// Create a completed task fixture
pub fn completed_task_fixture() -> TestFixture {
    let mut fixture = TestFixture::task();
    fixture
        .data
        .insert("status".to_string(), "completed".to_string());
    fixture
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_task_fixture() {
        let fixture = basic_task_fixture();
        assert_eq!(fixture.name, "task");
    }

    #[test]
    fn test_task_fixture_with_id() {
        let task_id = TaskId::new_v4();
        let fixture = task_fixture_with_id(task_id);
        assert!(fixture.get_data("task_id").is_some());
    }

    #[test]
    fn test_pending_task_fixture() {
        let fixture = pending_task_fixture();
        assert_eq!(fixture.get_data("status"), Some(&"pending".to_string()));
    }

    #[test]
    fn test_running_task_fixture() {
        let fixture = running_task_fixture();
        assert_eq!(fixture.get_data("status"), Some(&"running".to_string()));
    }

    #[test]
    fn test_completed_task_fixture() {
        let fixture = completed_task_fixture();
        assert_eq!(fixture.get_data("status"), Some(&"completed".to_string()));
    }
}
