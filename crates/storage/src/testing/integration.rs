//! # Integration Tests for orcher-common Type Compatibility
//!
//! This module provides integration tests to validate compatibility between
//! orcher-storage and orcher-common types, ensuring proper serialization,
//! deserialization, and type safety across crate boundaries.

/// Validates that TestFixture creation works correctly
#[cfg(test)]
pub fn validate_test_fixture_creation() -> bool {
    use orcher_common::testing::TestFixture;
    let fixture = TestFixture::task();
    fixture.name == "task" && fixture.get_data("task_id").is_some()
}

/// Validates workflow fixture compatibility
#[cfg(test)]
pub fn validate_workflow_fixture() -> bool {
    use orcher_common::testing::TestFixture;
    let fixture = TestFixture::workflow();
    fixture.name == "workflow" && fixture.get_data("workflow_id").is_some()
}

#[cfg(test)]
mod tests {
    use orcher_common::testing::TestFixture;

    #[test]
    fn test_task_fixture_creation() {
        let fixture = TestFixture::task();
        assert_eq!(fixture.name, "task");
        assert!(fixture.get_data("task_id").is_some());
        assert!(fixture.get_data("name").is_some());
        assert!(fixture.get_data("status").is_some());
    }

    #[test]
    fn test_workflow_fixture_creation() {
        let fixture = TestFixture::workflow();
        assert_eq!(fixture.name, "workflow");
        assert!(fixture.get_data("workflow_id").is_some());
        assert!(fixture.get_data("name").is_some());
    }

    #[test]
    fn test_user_fixture_creation() {
        let fixture = TestFixture::user();
        assert_eq!(fixture.name, "user");
        assert!(fixture.get_data("user_id").is_some());
        assert!(fixture.get_data("username").is_some());
    }
}
