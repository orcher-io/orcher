//! # Registry Service - DISABLED (Code-First Model)
//!
//! **CODE-FIRST ONLY MODEL**
//!
//! This service has been disabled as part of the code-first architecture cleanup.
//! All platform-based workflow registration and deployment has been removed.
//!
//! ## Migration to Code-First
//!
//! Use the `#[workflow]` macro to register workflows at compile-time:
//!
//! ```rust,ignore
//! #[workflow(name = "my_workflow", version = "1.0.0")]
//! async fn my_workflow(ctx: WorkflowContext) -> Result<()> {
//!     // Automatically registered in GLOBAL_REGISTRY
//! }
//! ```

use orcher_common::error::{OrcherError, OrcherResult};

/// Registry service for the API gateway layer
///
/// **DISABLED**: All methods return errors indicating platform operations are not supported.
#[derive(Clone, Default)]
pub struct RegistryService;

impl RegistryService {
    /// Create a new registry service
    #[must_use]
    pub fn new() -> Self {
        Self
    }

    /// Register a workflow - DISABLED
    ///
    /// Returns an error indicating platform registration is not supported.
    pub async fn register_workflow(
        &self,
        _manifest: serde_json::Value,
        _source: serde_json::Value,
        _user_id: uuid::Uuid,
        _audit_ctx: crate::api::rest::extractors::AuditContext,
    ) -> OrcherResult<uuid::Uuid> {
        Err(OrcherError::invalid_input(
            "Platform-based workflow registration is disabled. \
             Use compile-time registration via #[workflow] macro instead."
                .to_string(),
        ))
    }

    /// Deploy a workflow - DISABLED
    ///
    /// Returns an error indicating platform deployment is not supported.
    pub async fn deploy_workflow(
        &self,
        _id: uuid::Uuid,
        _environment: &str,
        _user_id: uuid::Uuid,
        _audit_ctx: crate::api::rest::extractors::AuditContext,
    ) -> OrcherResult<uuid::Uuid> {
        Err(OrcherError::invalid_input(
            "Platform-based workflow deployment is disabled. \
             Deploy your service binary directly to your infrastructure (K8s/ECS/Docker)."
                .to_string(),
        ))
    }

    /// Get workflow by ID - DISABLED
    ///
    /// Returns an error indicating platform registry lookups are not supported.
    pub async fn get_workflow(
        &self,
        _id: uuid::Uuid,
        _user_id: uuid::Uuid,
    ) -> OrcherResult<serde_json::Value> {
        Err(OrcherError::invalid_input(
            "Platform registry lookups are disabled. \
             Use code-first workflows registered via #[workflow] macro."
                .to_string(),
        ))
    }

    /// List workflows - DISABLED
    ///
    /// Returns an error indicating platform registry is not supported.
    pub async fn list_workflows(
        &self,
        _filters: serde_json::Value,
        _pagination: orcher_storage::repositories::base::pagination::Pagination,
        _user_id: uuid::Uuid,
    ) -> OrcherResult<orcher_storage::repositories::base::pagination::Page<serde_json::Value>> {
        Err(OrcherError::invalid_input(
            "Platform registry is disabled. \
             Use code-first workflows registered via #[workflow] macro."
                .to_string(),
        ))
    }

    /// Update workflow - DISABLED
    pub async fn update_workflow(
        &self,
        _id: uuid::Uuid,
        _updates: serde_json::Value,
        _user_id: uuid::Uuid,
        _audit_ctx: crate::api::rest::extractors::AuditContext,
    ) -> OrcherResult<()> {
        Err(OrcherError::invalid_input(
            "Platform registry operations are disabled in code-first model.".to_string(),
        ))
    }

    /// Delete workflow - DISABLED
    pub async fn delete_workflow(
        &self,
        _id: uuid::Uuid,
        _user_id: uuid::Uuid,
        _audit_ctx: crate::api::rest::extractors::AuditContext,
    ) -> OrcherResult<()> {
        Err(OrcherError::invalid_input(
            "Platform registry operations are disabled in code-first model.".to_string(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_register_workflow_disabled() {
        let service = RegistryService::new();

        let result = service
            .register_workflow(
                serde_json::json!({}),
                serde_json::json!({}),
                uuid::Uuid::new_v4(),
                crate::api::rest::extractors::AuditContext::default(),
            )
            .await;

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("disabled"));
    }

    #[tokio::test]
    async fn test_deploy_workflow_disabled() {
        let service = RegistryService::new();

        let result = service
            .deploy_workflow(
                uuid::Uuid::new_v4(),
                "production",
                uuid::Uuid::new_v4(),
                crate::api::rest::extractors::AuditContext::default(),
            )
            .await;

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("disabled"));
    }
}
