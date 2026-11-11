//! Namespace storage types.

use orcher_common::{
    types::time::Timestamptz,
    validation::{Validatable, ValidationResult},
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "postgres", derive(sqlx::FromRow))]
pub struct Namespace {
    pub id: Uuid,
    /// Unique within organization
    pub name: String,
    pub description: String,
    /// "active", "deprecated", or "deleted"
    pub status: String,
    pub owner_email: Option<String>,
    /// Days to retain completed workflow data
    pub retention_period_days: i32,
    pub data: serde_json::Value,
    /// NULL for system namespaces
    pub organization_id: Option<Uuid>,
    pub created_at: Timestamptz,
    pub updated_at: Timestamptz,
    pub deleted_at: Option<Timestamptz>,
}

impl Namespace {
    pub fn new(name: String, retention_period_days: i32) -> Self {
        let now = Timestamptz::now();
        Self {
            id: Uuid::new_v4(),
            name,
            description: String::new(),
            status: "active".to_string(),
            owner_email: None,
            retention_period_days,
            data: serde_json::json!({}),
            organization_id: None,
            created_at: now,
            updated_at: now,
            deleted_at: None,
        }
    }

    pub fn is_active(&self) -> bool {
        self.status == "active" && self.deleted_at.is_none()
    }

    pub fn is_deprecated(&self) -> bool {
        self.status == "deprecated"
    }

    pub fn is_deleted(&self) -> bool {
        self.deleted_at.is_some() || self.status == "deleted"
    }

    pub fn deprecate(&mut self) {
        self.status = "deprecated".to_string();
        self.updated_at = Timestamptz::now();
    }

    pub fn soft_delete(&mut self) {
        self.status = "deleted".to_string();
        self.deleted_at = Some(Timestamptz::now());
        self.updated_at = Timestamptz::now();
    }
}

impl Validatable for Namespace {
    fn validate(&self) -> ValidationResult {
        let mut result = ValidationResult::new();

        // Validate name
        if self.name.is_empty() {
            result.add_error("name", "Namespace name cannot be empty");
        }
        if self.name.len() > 100 {
            result.add_error("name", "Namespace name cannot exceed 100 characters");
        }
        if !self
            .name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
        {
            result.add_error(
                "name",
                "Namespace name can only contain alphanumeric characters, hyphens, and underscores",
            );
        }

        // Validate retention period
        if self.retention_period_days <= 0 {
            result.add_error("retention_period_days", "Retention period must be positive");
        }

        // Validate status
        if !matches!(self.status.as_str(), "active" | "deprecated" | "deleted") {
            result.add_error(
                "status",
                "Status must be 'active', 'deprecated', or 'deleted'",
            );
        }

        result
    }
}
