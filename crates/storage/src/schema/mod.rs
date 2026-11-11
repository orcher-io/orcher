//! # Schema Module
//!
//! This module contains all storage schema definitions, entity types, and validation
//! logic using orcher-common types for maximum compatibility across the ORCHER ecosystem.

/// Storage entity type definitions using orcher-common types
/// Declared value sets for columns guarded by a database CHECK constraint.
pub mod enums;

pub mod types;

// Re-export commonly used types for convenience
pub use types::Workflow;

#[cfg(test)]
mod tests {
    use super::*;
    use orcher_common::types::identifiers::{traits::Identifier, OrganizationId, UserId};

    #[test]
    fn test_schema_module_exports() {
        // Verify all types are properly exported and work with orcher-common
        let org_id = OrganizationId::new_v4();
        let user_id = UserId::new_v4();
        let workflow = Workflow::new(
            org_id,
            "test-workflow".to_string(),
            serde_json::json!({"steps": []}),
            user_id,
        );

        assert!(workflow.id.is_valid());
    }
}
