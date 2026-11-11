//! Workflow definition storage types.

use orcher_common::types::time::Timestamptz;
use orcher_common::{
    types::identifiers::{OrganizationId, UserId, WorkflowId},
    validation::{Validatable, ValidationResult},
};
use serde::{Deserialize, Serialize};

/// Lifecycle status of a workflow definition (not execution state).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "postgres", derive(sqlx::Type))]
#[cfg_attr(
    feature = "postgres",
    sqlx(type_name = "varchar", rename_all = "lowercase")
)]
pub enum WorkflowLifecycleStatus {
    Draft,
    Active,
    Deprecated,
    Archived,
}

impl Default for WorkflowLifecycleStatus {
    fn default() -> Self {
        Self::Draft
    }
}

impl std::fmt::Display for WorkflowLifecycleStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Draft => write!(f, "draft"),
            Self::Active => write!(f, "active"),
            Self::Deprecated => write!(f, "deprecated"),
            Self::Archived => write!(f, "archived"),
        }
    }
}

/// Persisted workflow definition. See `WorkflowExecution` for runtime state.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "postgres", derive(sqlx::FromRow))]
pub struct Workflow {
    pub id: WorkflowId,
    pub organization_id: OrganizationId,
    pub name: String,
    pub description: Option<String>,
    /// Semver format (e.g., "1.0.0")
    pub version: String,
    pub status: WorkflowLifecycleStatus,
    /// DAG structure as JSON
    pub definition: serde_json::Value,
    pub config: serde_json::Value,
    pub created_by: UserId,
    pub modified_by: Option<UserId>,
    pub created_at: Timestamptz,
    pub updated_at: Timestamptz,
    pub deleted_at: Option<Timestamptz>,
}

impl Workflow {
    pub fn new(
        organization_id: OrganizationId,
        name: String,
        definition: serde_json::Value,
        created_by: UserId,
    ) -> Self {
        let now = Timestamptz::now();
        Self {
            id: WorkflowId::new_v4(),
            organization_id,
            name,
            description: None,
            version: "1.0.0".to_string(),
            status: WorkflowLifecycleStatus::default(),
            definition,
            config: serde_json::Value::Object(Default::default()),
            created_by,
            modified_by: None,
            created_at: now,
            updated_at: now,
            deleted_at: None,
        }
    }

    pub fn is_deleted(&self) -> bool {
        self.deleted_at.is_some()
    }

    pub fn soft_delete(&mut self) {
        self.deleted_at = Some(Timestamptz::now());
        self.updated_at = Timestamptz::now();
    }

    pub fn restore(&mut self) {
        self.deleted_at = None;
        self.updated_at = Timestamptz::now();
    }

    pub fn activate(&mut self) {
        self.status = WorkflowLifecycleStatus::Active;
        self.updated_at = Timestamptz::now();
    }

    pub fn deprecate(&mut self) {
        self.status = WorkflowLifecycleStatus::Deprecated;
        self.updated_at = Timestamptz::now();
    }

    pub fn archive(&mut self) {
        self.status = WorkflowLifecycleStatus::Archived;
        self.updated_at = Timestamptz::now();
    }
}

impl Validatable for Workflow {
    fn validate(&self) -> ValidationResult {
        let mut result = ValidationResult::new();

        // Validate workflow name is not empty
        if self.name.trim().is_empty() {
            result.add_error("name", "Workflow name cannot be empty");
        }

        // Validate workflow name length
        if self.name.len() > 255 {
            result.add_error("name", "Workflow name cannot exceed 255 characters");
        }

        // Validate version format (basic semver check)
        if !self.version.contains('.') {
            result.add_error(
                "version",
                "Version must be in semver format (e.g., '1.0.0')",
            );
        }

        // Validate definition is not empty
        if self.definition.is_null() {
            result.add_error("definition", "Workflow definition cannot be null");
        }

        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use orcher_common::types::identifiers::traits::Identifier;

    #[test]
    fn test_workflow_creation() {
        let definition = serde_json::json!({"steps": []});
        let org_id = OrganizationId::new_v4();
        let user_id = UserId::new_v4();
        let workflow = Workflow::new(org_id, "test-workflow".to_string(), definition, user_id);

        assert!(workflow.id.is_valid());
        assert_eq!(workflow.name, "test-workflow");
        assert_eq!(workflow.status, WorkflowLifecycleStatus::Draft);
        assert_eq!(workflow.created_by, user_id);
        assert!(workflow.deleted_at.is_none());
    }

    #[test]
    fn test_workflow_serialization() {
        let definition = serde_json::json!({"steps": [{"type": "task"}]});
        let org_id = OrganizationId::new_v4();
        let user_id = UserId::new_v4();
        let workflow = Workflow::new(org_id, "serialize-test".to_string(), definition, user_id);

        let serialized = serde_json::to_string(&workflow).unwrap();
        let deserialized: Workflow = serde_json::from_str(&serialized).unwrap();

        assert_eq!(workflow.id, deserialized.id);
        assert_eq!(workflow.name, deserialized.name);
        assert_eq!(workflow.created_by, deserialized.created_by);
    }

    #[test]
    fn test_workflow_validation_success() {
        let definition = serde_json::json!({"steps": [{"type": "task"}]});
        let org_id = OrganizationId::new_v4();
        let user_id = UserId::new_v4();
        let workflow = Workflow::new(org_id, "valid-workflow".to_string(), definition, user_id);

        let validation_result = workflow.validate();
        assert!(validation_result.is_valid());
    }

    #[test]
    fn test_workflow_validation_empty_name() {
        let definition = serde_json::json!({"steps": []});
        let org_id = OrganizationId::new_v4();
        let user_id = UserId::new_v4();
        let workflow = Workflow::new(org_id, "".to_string(), definition, user_id);

        let validation_result = workflow.validate();
        assert!(!validation_result.is_valid());
        assert!(!validation_result.errors_for_field("name").is_empty());
    }

    #[test]
    fn test_workflow_validation_invalid_version() {
        let definition = serde_json::json!({"steps": []});
        let org_id = OrganizationId::new_v4();
        let user_id = UserId::new_v4();
        let mut workflow = Workflow::new(org_id, "test-workflow".to_string(), definition, user_id);
        workflow.version = "invalid".to_string();

        let validation_result = workflow.validate();
        assert!(!validation_result.is_valid());
        assert!(!validation_result.errors_for_field("version").is_empty());
    }

    #[test]
    fn test_workflow_soft_delete() {
        let definition = serde_json::json!({"steps": []});
        let org_id = OrganizationId::new_v4();
        let user_id = UserId::new_v4();
        let mut workflow = Workflow::new(org_id, "test-workflow".to_string(), definition, user_id);

        assert!(!workflow.is_deleted());

        workflow.soft_delete();
        assert!(workflow.is_deleted());
        assert!(workflow.deleted_at.is_some());

        workflow.restore();
        assert!(!workflow.is_deleted());
        assert!(workflow.deleted_at.is_none());
    }

    #[test]
    fn test_workflow_lifecycle_transitions() {
        let definition = serde_json::json!({"steps": []});
        let org_id = OrganizationId::new_v4();
        let user_id = UserId::new_v4();
        let mut workflow = Workflow::new(org_id, "test-workflow".to_string(), definition, user_id);

        assert_eq!(workflow.status, WorkflowLifecycleStatus::Draft);

        workflow.activate();
        assert_eq!(workflow.status, WorkflowLifecycleStatus::Active);

        workflow.deprecate();
        assert_eq!(workflow.status, WorkflowLifecycleStatus::Deprecated);

        workflow.archive();
        assert_eq!(workflow.status, WorkflowLifecycleStatus::Archived);
    }

    #[test]
    fn test_workflow_lifecycle_status_display() {
        assert_eq!(WorkflowLifecycleStatus::Draft.to_string(), "draft");
        assert_eq!(WorkflowLifecycleStatus::Active.to_string(), "active");
        assert_eq!(
            WorkflowLifecycleStatus::Deprecated.to_string(),
            "deprecated"
        );
        assert_eq!(WorkflowLifecycleStatus::Archived.to_string(), "archived");
    }
}
