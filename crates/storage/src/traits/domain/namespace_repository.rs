//! # Namespace Repository Trait
//!
//! Domain-specific trait for namespace data persistence operations.

use async_trait::async_trait;
use orcher_common::OrcherResult;
use uuid::Uuid;

use crate::schema::types::Namespace;

/// Namespace repository trait for CRUD operations on namespaces
#[async_trait]
pub trait NamespaceRepositoryTrait: Send + Sync {
    /// Create a new namespace
    async fn create(&self, namespace: Namespace) -> OrcherResult<Namespace>;

    /// Get a namespace by name (within an organization scope)
    async fn get_by_name(
        &self,
        name: &str,
        org_id: Option<&Uuid>,
    ) -> OrcherResult<Option<Namespace>>;

    /// List namespaces (within an organization scope)
    async fn list(
        &self,
        org_id: Option<&Uuid>,
        limit: i64,
        offset: i64,
    ) -> OrcherResult<Vec<Namespace>>;

    /// Count total namespaces (within an organization scope)
    async fn count(&self, org_id: Option<&Uuid>) -> OrcherResult<i64>;

    /// Update namespace metadata
    async fn update(&self, namespace: Namespace) -> OrcherResult<Namespace>;

    /// Update namespace status
    async fn update_status(
        &self,
        name: &str,
        org_id: Option<&Uuid>,
        status: &str,
    ) -> OrcherResult<()>;

    /// Soft-delete a namespace
    async fn delete(&self, name: &str, org_id: Option<&Uuid>) -> OrcherResult<()>;

    /// Check if a namespace exists and is active
    async fn exists(&self, name: &str, org_id: Option<&Uuid>) -> OrcherResult<bool>;
}
