//! # Worker Repository Domain Trait
//!
//! Domain-specific repository operations for workers using orcher-common types
//! for maximum compatibility and type safety across the ORCHER ecosystem.

use async_trait::async_trait;
use orcher_common::{
    types::identifiers::{OrganizationId, WorkerId},
    OrcherResult,
};

use crate::{schema::types::Worker, traits::core::Repository};

/// Domain-specific repository trait for Worker entities
///
/// Worker-specific operations for data persistence.
/// Extends the base Repository trait with worker-specific operations.
#[async_trait]
pub trait WorkerRepositoryTrait: Repository<Worker> {
    /// Find workers by status
    /// Scoped to a specific organization for multi-tenancy
    async fn find_by_status(
        &self,
        organization_id: &OrganizationId,
        status: &str,
    ) -> OrcherResult<Vec<Worker>>;

    /// Find active workers (available for task assignment)
    /// Scoped to a specific organization for multi-tenancy
    async fn find_active_workers(
        &self,
        organization_id: &OrganizationId,
    ) -> OrcherResult<Vec<Worker>>;

    /// Update worker status
    /// Scoped to a specific organization for multi-tenancy
    async fn update_status(
        &self,
        organization_id: &OrganizationId,
        worker_id: WorkerId,
        status: &str,
    ) -> OrcherResult<()>;

    /// Record worker heartbeat
    /// Scoped to a specific organization for multi-tenancy
    async fn update_heartbeat(
        &self,
        organization_id: &OrganizationId,
        worker_id: WorkerId,
    ) -> OrcherResult<()>;

    /// Find workers by capability
    /// Scoped to a specific organization for multi-tenancy
    async fn find_by_capability(
        &self,
        organization_id: &OrganizationId,
        capability: &str,
    ) -> OrcherResult<Vec<Worker>>;

    /// Update worker capacity
    /// Scoped to a specific organization for multi-tenancy
    async fn update_capacity(
        &self,
        organization_id: &OrganizationId,
        worker_id: WorkerId,
        current_load: i32,
        max_capacity: i32,
    ) -> OrcherResult<()>;

    /// Find workers with available capacity
    /// Scoped to a specific organization for multi-tenancy
    async fn find_available_workers(
        &self,
        organization_id: &OrganizationId,
    ) -> OrcherResult<Vec<Worker>>;

    /// Find workers that haven't sent heartbeat recently
    /// Scoped to a specific organization for multi-tenancy
    async fn find_stale_workers(
        &self,
        organization_id: &OrganizationId,
        threshold_seconds: i64,
    ) -> OrcherResult<Vec<Worker>>;

    /// Update worker metadata
    /// Scoped to a specific organization for multi-tenancy
    async fn update_metadata(
        &self,
        organization_id: &OrganizationId,
        worker_id: WorkerId,
        metadata: serde_json::Value,
    ) -> OrcherResult<()>;

    /// Count workers by status
    /// Scoped to a specific organization for multi-tenancy
    async fn count_by_status(
        &self,
        organization_id: &OrganizationId,
        status: &str,
    ) -> OrcherResult<u64>;

    /// Find workers by tag
    /// Scoped to a specific organization for multi-tenancy
    async fn find_by_tag(
        &self,
        organization_id: &OrganizationId,
        tag: &str,
    ) -> OrcherResult<Vec<Worker>>;

    /// Deactivate worker
    /// Scoped to a specific organization for multi-tenancy
    async fn deactivate_worker(
        &self,
        organization_id: &OrganizationId,
        worker_id: WorkerId,
    ) -> OrcherResult<()>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_worker_repository_trait_bounds() {
        // Compile-time test to ensure trait bounds work correctly
        fn _assert_worker_repository_bounds<R>()
        where
            R: WorkerRepositoryTrait,
        {
        }
    }

    #[test]
    fn test_worker_repository_types() {
        // Compile-time test to ensure associated types work correctly
        fn _assert_worker_types<R>()
        where
            R: WorkerRepositoryTrait<Id = WorkerId>,
        {
        }
    }
}
