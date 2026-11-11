//! # Namespace Repository - PostgreSQL Implementation
//!
//! Handles persistence for namespace entities.
//! Implements the NamespaceRepositoryTrait for PostgreSQL backend.

use async_trait::async_trait;
use dashmap::DashMap;
use orcher_common::{error::OrcherError, OrcherResult};
use sqlx::PgPool;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{debug, info};
use uuid::Uuid;

use crate::{schema::types::Namespace, traits::domain::NamespaceRepositoryTrait};

/// How long a positive `exists` result stays cached. Namespaces change rarely,
/// but the poll path checks existence on every poll (~8 per workflow), so a short
/// TTL removes almost all of those round-trips while bounding staleness.
const NAMESPACE_EXISTS_TTL: Duration = Duration::from_secs(10);

/// PostgreSQL implementation of NamespaceRepository
#[derive(Debug, Clone)]
pub struct NamespaceRepository {
    pool: PgPool,
    /// Positive-result TTL cache for [`exists`]. Only `true` (active) results are
    /// cached, so a just-created namespace is never wrongly rejected; a
    /// deprecated/deleted one stops being served within `NAMESPACE_EXISTS_TTL`
    /// (and immediately on the instance that performed the change). Shared across
    /// clones via `Arc`.
    exists_cache: Arc<DashMap<(String, Option<Uuid>), Instant>>,
}

impl NamespaceRepository {
    /// Create a new repository instance
    pub fn new(pool: PgPool) -> Self {
        Self {
            pool,
            exists_cache: Arc::new(DashMap::new()),
        }
    }

    /// Get reference to the connection pool
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }
}

#[async_trait]
impl NamespaceRepositoryTrait for NamespaceRepository {
    async fn create(&self, ns: Namespace) -> OrcherResult<Namespace> {
        debug!("Creating namespace: name={}", ns.name);

        sqlx::query(
            r#"
            INSERT INTO namespaces (
                id, name, description, status, owner_email,
                retention_period_days, data, organization_id,
                created_at, updated_at, deleted_at
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
            "#,
        )
        .bind(ns.id)
        .bind(&ns.name)
        .bind(&ns.description)
        .bind(&ns.status)
        .bind(&ns.owner_email)
        .bind(ns.retention_period_days)
        .bind(&ns.data)
        .bind(ns.organization_id)
        .bind(ns.created_at)
        .bind(ns.updated_at)
        .bind(ns.deleted_at)
        .execute(&self.pool)
        .await
        .map_err(|e| OrcherError::internal(format!("Failed to create namespace: {}", e)))?;

        info!("Namespace created successfully: name={}", ns.name);
        Ok(ns)
    }

    async fn get_by_name(
        &self,
        name: &str,
        org_id: Option<&Uuid>,
    ) -> OrcherResult<Option<Namespace>> {
        debug!("Getting namespace: name={}", name);

        let result = match org_id {
            Some(oid) => {
                sqlx::query_as::<_, Namespace>(
                    concat!("SELECT ", crate::repositories::columns::namespaces!(), " FROM namespaces WHERE name = $1 AND organization_id = $2 AND status != 'deleted'"),
                )
                .bind(name)
                .bind(oid)
                .fetch_optional(&self.pool)
                .await
            }
            None => {
                sqlx::query_as::<_, Namespace>(
                    concat!("SELECT ", crate::repositories::columns::namespaces!(), " FROM namespaces WHERE name = $1 AND organization_id IS NULL AND status != 'deleted'"),
                )
                .bind(name)
                .fetch_optional(&self.pool)
                .await
            }
        };

        result.map_err(|e| OrcherError::internal(format!("Failed to get namespace: {}", e)))
    }

    async fn list(
        &self,
        org_id: Option<&Uuid>,
        limit: i64,
        offset: i64,
    ) -> OrcherResult<Vec<Namespace>> {
        debug!(
            "Listing namespaces: org_id={:?}, limit={}, offset={}",
            org_id, limit, offset
        );

        let result = match org_id {
            Some(oid) => {
                sqlx::query_as::<_, Namespace>(
                    concat!("SELECT ", crate::repositories::columns::namespaces!(), " FROM namespaces WHERE organization_id = $1 AND status != 'deleted' ORDER BY name LIMIT $2 OFFSET $3"),
                )
                .bind(oid)
                .bind(limit)
                .bind(offset)
                .fetch_all(&self.pool)
                .await
            }
            None => {
                sqlx::query_as::<_, Namespace>(
                    concat!("SELECT ", crate::repositories::columns::namespaces!(), " FROM namespaces WHERE status != 'deleted' ORDER BY name LIMIT $1 OFFSET $2"),
                )
                .bind(limit)
                .bind(offset)
                .fetch_all(&self.pool)
                .await
            }
        };

        result.map_err(|e| OrcherError::internal(format!("Failed to list namespaces: {}", e)))
    }

    async fn count(&self, org_id: Option<&Uuid>) -> OrcherResult<i64> {
        let row = match org_id {
            Some(oid) => {
                sqlx::query_scalar::<_, i64>(
                    "SELECT COUNT(*) FROM namespaces WHERE organization_id = $1 AND status != 'deleted'",
                )
                .bind(oid)
                .fetch_one(&self.pool)
                .await
            }
            None => {
                sqlx::query_scalar::<_, i64>(
                    "SELECT COUNT(*) FROM namespaces WHERE status != 'deleted'",
                )
                .fetch_one(&self.pool)
                .await
            }
        };

        row.map_err(|e| OrcherError::internal(format!("Failed to count namespaces: {}", e)))
    }

    async fn update(&self, ns: Namespace) -> OrcherResult<Namespace> {
        debug!("Updating namespace: name={}", ns.name);

        sqlx::query(
            r#"
            UPDATE namespaces
            SET description = $1, owner_email = $2, retention_period_days = $3, data = $4
            WHERE name = $5 AND (organization_id = $6 OR ($6 IS NULL AND organization_id IS NULL))
              AND status != 'deleted'
            "#,
        )
        .bind(&ns.description)
        .bind(&ns.owner_email)
        .bind(ns.retention_period_days)
        .bind(&ns.data)
        .bind(&ns.name)
        .bind(ns.organization_id)
        .execute(&self.pool)
        .await
        .map_err(|e| OrcherError::internal(format!("Failed to update namespace: {}", e)))?;

        // Fetch updated record (trigger sets updated_at)
        self.get_by_name(&ns.name, ns.organization_id.as_ref())
            .await?
            .ok_or_else(|| OrcherError::not_found("Namespace", &ns.name))
    }

    async fn update_status(
        &self,
        name: &str,
        org_id: Option<&Uuid>,
        status: &str,
    ) -> OrcherResult<()> {
        debug!(
            "Updating namespace status: name={}, status={}",
            name, status
        );

        let deleted_at = if status == "deleted" {
            Some(orcher_common::types::time::Timestamptz::now())
        } else {
            None
        };

        sqlx::query(
            r#"
            UPDATE namespaces
            SET status = $1, deleted_at = $2
            WHERE name = $3 AND (organization_id = $4 OR ($4 IS NULL AND organization_id IS NULL))
            "#,
        )
        .bind(status)
        .bind(deleted_at)
        .bind(name)
        .bind(org_id)
        .execute(&self.pool)
        .await
        .map_err(|e| OrcherError::internal(format!("Failed to update namespace status: {}", e)))?;

        // Drop any cached positive so a deprecate/delete takes effect immediately
        // on this instance (other instances catch up within the TTL).
        self.exists_cache
            .remove(&(name.to_string(), org_id.copied()));

        info!("Namespace status updated: name={}, status={}", name, status);
        Ok(())
    }

    async fn delete(&self, name: &str, org_id: Option<&Uuid>) -> OrcherResult<()> {
        self.update_status(name, org_id, "deleted").await
    }

    async fn exists(&self, name: &str, org_id: Option<&Uuid>) -> OrcherResult<bool> {
        let key = (name.to_string(), org_id.copied());

        // Fast path: a fresh positive cache entry.
        if let Some(entry) = self.exists_cache.get(&key) {
            if *entry.value() > Instant::now() {
                return Ok(true);
            }
        }

        let result = match org_id {
            Some(oid) => {
                sqlx::query_scalar::<_, bool>(
                    "SELECT EXISTS(SELECT 1 FROM namespaces WHERE name = $1 AND organization_id = $2 AND status = 'active')",
                )
                .bind(name)
                .bind(oid)
                .fetch_one(&self.pool)
                .await
            }
            None => {
                sqlx::query_scalar::<_, bool>(
                    "SELECT EXISTS(SELECT 1 FROM namespaces WHERE name = $1 AND organization_id IS NULL AND status = 'active')",
                )
                .bind(name)
                .fetch_one(&self.pool)
                .await
            }
        };

        let exists = result.map_err(|e| {
            OrcherError::internal(format!("Failed to check namespace existence: {}", e))
        })?;

        // Cache positive results only. Negatives are never cached so a namespace
        // created moments later is usable immediately.
        if exists {
            self.exists_cache
                .insert(key, Instant::now() + NAMESPACE_EXISTS_TTL);
        }

        Ok(exists)
    }
}
