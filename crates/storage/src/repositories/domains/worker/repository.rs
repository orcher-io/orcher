//! # Worker Domain Repository
//!
//! Worker persistence implementation using PostgreSQL backend.
//! Handles CRUD operations and worker-specific queries with proper database mapping.

use async_trait::async_trait;
use orcher_common::types::time::Timestamptz;
use orcher_common::{
    types::identifiers::{OrganizationId, WorkerId},
    OrcherResult,
};
use sqlx::PgPool;
use std::str::FromStr;
use tracing::{debug, info};
use uuid::Uuid;

use crate::{
    repositories::base::CrudOperations,
    schema::types::worker::{Worker, WorkerStatus},
    traits::{core::Repository, domain::WorkerRepositoryTrait},
};

/// Internal row type for SQL deserialization
#[derive(Debug, Clone, sqlx::FromRow)]
struct WorkerRow {
    id: Uuid,
    organization_id: Uuid,
    worker_id: String,
    name: String,
    worker_type: String,
    status: String,
    capabilities: Option<Vec<String>>,
    max_concurrent_tasks: i32,
    current_task_count: i32,
    cpu_cores: Option<f64>,
    memory_mb: Option<i32>,
    disk_space_mb: Option<i32>,
    metadata: serde_json::Value,
    tags: Option<Vec<String>>,
    host_address: Option<String>,
    port: Option<i32>,
    last_heartbeat: Option<Timestamptz>,
    health_status: String,
    health_details: serde_json::Value,
    created_at: Timestamptz,
    updated_at: Timestamptz,
    registered_at: Option<Timestamptz>,
}

impl TryFrom<WorkerRow> for Worker {
    type Error = orcher_common::error::OrcherError;

    fn try_from(row: WorkerRow) -> Result<Self, Self::Error> {
        let status = WorkerStatus::from_str(&row.status).map_err(|e| {
            orcher_common::error::OrcherError::internal(format!(
                "Invalid worker status '{}': {}",
                row.status, e
            ))
        })?;

        Ok(Worker {
            id: WorkerId::from_uuid(row.id),
            organization_id: OrganizationId::from_uuid(row.organization_id),
            worker_id: row.worker_id,
            name: row.name,
            worker_type: row.worker_type,
            status,
            capabilities: row.capabilities.unwrap_or_default(),
            max_concurrent_tasks: row.max_concurrent_tasks,
            current_task_count: row.current_task_count,
            cpu_cores: row.cpu_cores,
            memory_mb: row.memory_mb,
            disk_space_mb: row.disk_space_mb,
            metadata: row.metadata,
            tags: row.tags.unwrap_or_default(),
            host_address: row.host_address,
            port: row.port,
            last_heartbeat: row.last_heartbeat,
            health_status: row.health_status,
            health_details: row.health_details,
            created_at: row.created_at,
            updated_at: row.updated_at,
            registered_at: row.registered_at,
        })
    }
}

/// Helper to convert Worker to SQL parameters
#[allow(clippy::type_complexity)]
fn worker_to_params(
    worker: &Worker,
) -> (
    Uuid,
    Uuid,
    String,
    String,
    String,
    String,
    Vec<String>,
    i32,
    i32,
    Option<f64>,
    Option<i32>,
    Option<i32>,
    serde_json::Value,
    Vec<String>,
    Option<String>,
    Option<i32>,
    Option<Timestamptz>,
    String,
    serde_json::Value,
) {
    (
        worker.id.as_uuid(),
        worker.organization_id.as_uuid(),
        worker.worker_id.clone(),
        worker.name.clone(),
        worker.worker_type.clone(),
        worker.status.to_string(),
        worker.capabilities.clone(),
        worker.max_concurrent_tasks,
        worker.current_task_count,
        worker.cpu_cores,
        worker.memory_mb,
        worker.disk_space_mb,
        worker.metadata.clone(),
        worker.tags.clone(),
        worker.host_address.clone(),
        worker.port,
        worker.last_heartbeat,
        worker.health_status.clone(),
        worker.health_details.clone(),
    )
}

/// PostgreSQL implementation of WorkerRepository
#[derive(Debug, Clone)]
pub struct WorkerRepository {
    pool: PgPool,
}

impl WorkerRepository {
    /// Create a new WorkerRepository instance without caching
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Get a reference to the connection pool
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    // ========== QUERY METHODS ==========

    /// Find worker by business ID (worker_id string)
    pub async fn find_by_worker_id(&self, worker_id: &str) -> OrcherResult<Option<Worker>> {
        debug!("Finding worker by worker_id: {}", worker_id);

        let row: Option<WorkerRow> = sqlx::query_as(
            r#"
            SELECT id, organization_id, worker_id, name, worker_type, status,
                   capabilities, max_concurrent_tasks, current_task_count,
                   cpu_cores, memory_mb, disk_space_mb,
                   metadata, tags,
                   host_address, port,
                   last_heartbeat, health_status, health_details,
                   created_at, updated_at, registered_at
            FROM workers
            WHERE worker_id = $1
            "#,
        )
        .bind(worker_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| {
            orcher_common::error::OrcherError::internal(format!(
                "Failed to find worker by worker_id: {}",
                e
            ))
        })?;

        row.map(|r| r.try_into()).transpose()
    }

    /// Find workers by status
    pub async fn find_by_status(&self, status: WorkerStatus) -> OrcherResult<Vec<Worker>> {
        debug!("Finding workers by status: {}", status);

        let rows: Vec<WorkerRow> = sqlx::query_as(
            r#"
            SELECT id, organization_id, worker_id, name, worker_type, status,
                   capabilities, max_concurrent_tasks, current_task_count,
                   cpu_cores, memory_mb, disk_space_mb,
                   metadata, tags,
                   host_address, port,
                   last_heartbeat, health_status, health_details,
                   created_at, updated_at, registered_at
            FROM workers
            WHERE status = $1
            ORDER BY created_at DESC
            "#,
        )
        .bind(status.to_string())
        .fetch_all(&self.pool)
        .await
        .map_err(|e| {
            orcher_common::error::OrcherError::internal(format!(
                "Failed to find workers by status: {}",
                e
            ))
        })?;

        rows.into_iter()
            .map(|r| r.try_into())
            .collect::<Result<Vec<_>, _>>()
    }

    /// Find available workers (idle or busy with capacity)
    pub async fn find_available(&self) -> OrcherResult<Vec<Worker>> {
        debug!("Finding available workers");

        let rows: Vec<WorkerRow> = sqlx::query_as(
            r#"
            SELECT id, organization_id, worker_id, name, worker_type, status,
                   capabilities, max_concurrent_tasks, current_task_count,
                   cpu_cores, memory_mb, disk_space_mb,
                   metadata, tags,
                   host_address, port,
                   last_heartbeat, health_status, health_details,
                   created_at, updated_at, registered_at
            FROM workers
            WHERE (status = 'idle' OR status = 'busy')
              AND current_task_count < max_concurrent_tasks
            ORDER BY current_task_count ASC, created_at ASC
            "#,
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|e| {
            orcher_common::error::OrcherError::internal(format!(
                "Failed to find available workers: {}",
                e
            ))
        })?;

        rows.into_iter()
            .map(|r| r.try_into())
            .collect::<Result<Vec<_>, _>>()
    }

    /// Find all workers
    pub async fn find_all(&self) -> OrcherResult<Vec<Worker>> {
        debug!("Finding all workers");

        let rows: Vec<WorkerRow> = sqlx::query_as(
            r#"
            SELECT id, organization_id, worker_id, name, worker_type, status,
                   capabilities, max_concurrent_tasks, current_task_count,
                   cpu_cores, memory_mb, disk_space_mb,
                   metadata, tags,
                   host_address, port,
                   last_heartbeat, health_status, health_details,
                   created_at, updated_at, registered_at
            FROM workers
            ORDER BY created_at DESC
            LIMIT 1000
            "#,
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|e| {
            orcher_common::error::OrcherError::internal(format!(
                "Failed to find all workers: {}",
                e
            ))
        })?;

        rows.into_iter()
            .map(|r| r.try_into())
            .collect::<Result<Vec<_>, _>>()
    }

    /// Find workers by tag
    pub async fn find_by_tag(&self, tag: &str) -> OrcherResult<Vec<Worker>> {
        debug!("Finding workers by tag: {}", tag);

        let rows: Vec<WorkerRow> = sqlx::query_as(
            r#"
            SELECT id, organization_id, worker_id, name, worker_type, status,
                   capabilities, max_concurrent_tasks, current_task_count,
                   cpu_cores, memory_mb, disk_space_mb,
                   metadata, tags,
                   host_address, port,
                   last_heartbeat, health_status, health_details,
                   created_at, updated_at, registered_at
            FROM workers
            WHERE $1 = ANY(tags)
            ORDER BY created_at DESC
            "#,
        )
        .bind(tag)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| {
            orcher_common::error::OrcherError::internal(format!(
                "Failed to find workers by tag: {}",
                e
            ))
        })?;

        rows.into_iter()
            .map(|r| r.try_into())
            .collect::<Result<Vec<_>, _>>()
    }

    /// Find workers by capability
    pub async fn find_by_capability(&self, capability: &str) -> OrcherResult<Vec<Worker>> {
        debug!("Finding workers by capability: {}", capability);

        let rows: Vec<WorkerRow> = sqlx::query_as(
            r#"
            SELECT id, organization_id, worker_id, name, worker_type, status,
                   capabilities, max_concurrent_tasks, current_task_count,
                   cpu_cores, memory_mb, disk_space_mb,
                   metadata, tags,
                   host_address, port,
                   last_heartbeat, health_status, health_details,
                   created_at, updated_at, registered_at
            FROM workers
            WHERE $1 = ANY(capabilities)
            ORDER BY created_at DESC
            "#,
        )
        .bind(capability)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| {
            orcher_common::error::OrcherError::internal(format!(
                "Failed to find workers by capability: {}",
                e
            ))
        })?;

        rows.into_iter()
            .map(|r| r.try_into())
            .collect::<Result<Vec<_>, _>>()
    }

    // ========== UPDATE METHODS ==========

    /// Update worker heartbeat
    pub async fn update_heartbeat(&self, id: WorkerId) -> OrcherResult<()> {
        debug!("Updating worker heartbeat: id={}", id);

        let result = sqlx::query(
            r#"
            UPDATE workers
            SET last_heartbeat = NOW(),
                updated_at = NOW()
            WHERE id = $1
            "#,
        )
        .bind(id.as_uuid())
        .execute(&self.pool)
        .await
        .map_err(|e| {
            orcher_common::error::OrcherError::internal(format!(
                "Failed to update heartbeat: {}",
                e
            ))
        })?;

        if result.rows_affected() == 0 {
            return Err(orcher_common::error::OrcherError::NotFound {
                resource_type: "Worker".to_string(),
                id: id.to_string(),
            });
        }

        Ok(())
    }

    /// Update worker status
    pub async fn update_status(&self, id: WorkerId, status: WorkerStatus) -> OrcherResult<()> {
        debug!("Updating worker status: id={}, status={}", id, status);

        let result = sqlx::query(
            r#"
            UPDATE workers
            SET status = $1,
                updated_at = NOW()
            WHERE id = $2
            "#,
        )
        .bind(status.to_string())
        .bind(id.as_uuid())
        .execute(&self.pool)
        .await
        .map_err(|e| {
            orcher_common::error::OrcherError::internal(format!("Failed to update status: {}", e))
        })?;

        if result.rows_affected() == 0 {
            return Err(orcher_common::error::OrcherError::NotFound {
                resource_type: "Worker".to_string(),
                id: id.to_string(),
            });
        }

        Ok(())
    }

    /// Update worker task counts
    pub async fn update_task_count(
        &self,
        id: WorkerId,
        current_task_count: i32,
    ) -> OrcherResult<()> {
        debug!(
            "Updating worker task count: id={}, count={}",
            id, current_task_count
        );

        let result = sqlx::query(
            r#"
            UPDATE workers
            SET current_task_count = $1,
                updated_at = NOW()
            WHERE id = $2
            "#,
        )
        .bind(current_task_count)
        .bind(id.as_uuid())
        .execute(&self.pool)
        .await
        .map_err(|e| {
            orcher_common::error::OrcherError::internal(format!(
                "Failed to update task count: {}",
                e
            ))
        })?;

        if result.rows_affected() == 0 {
            return Err(orcher_common::error::OrcherError::NotFound {
                resource_type: "Worker".to_string(),
                id: id.to_string(),
            });
        }

        Ok(())
    }

    /// Update worker health status
    pub async fn update_health(
        &self,
        id: WorkerId,
        health_status: String,
        health_details: serde_json::Value,
    ) -> OrcherResult<()> {
        debug!(
            "Updating worker health: id={}, status={}",
            id, health_status
        );

        let result = sqlx::query(
            r#"
            UPDATE workers
            SET health_status = $1,
                health_details = $2,
                updated_at = NOW()
            WHERE id = $3
            "#,
        )
        .bind(health_status)
        .bind(health_details)
        .bind(id.as_uuid())
        .execute(&self.pool)
        .await
        .map_err(|e| {
            orcher_common::error::OrcherError::internal(format!("Failed to update health: {}", e))
        })?;

        if result.rows_affected() == 0 {
            return Err(orcher_common::error::OrcherError::NotFound {
                resource_type: "Worker".to_string(),
                id: id.to_string(),
            });
        }

        Ok(())
    }

    // ========== UPSERT METHODS ==========

    /// Upsert a worker (insert or update on conflict by worker_id)
    ///
    /// Used by the worker registration gRPC service to persist worker state.
    /// If a worker with the same `worker_id` already exists, updates its fields
    /// instead of failing with a unique constraint violation.
    pub async fn upsert(&self, entity: &Worker) -> OrcherResult<()> {
        debug!("Upserting worker: worker_id={}", entity.worker_id);

        let params = worker_to_params(entity);

        sqlx::query(
            r#"
            INSERT INTO workers (
                id, organization_id, worker_id, name, worker_type, status,
                capabilities, max_concurrent_tasks, current_task_count,
                cpu_cores, memory_mb, disk_space_mb,
                metadata, tags,
                host_address, port,
                last_heartbeat, health_status, health_details,
                created_at, updated_at, registered_at
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19,
                NOW(), NOW(), NOW()
            )
            ON CONFLICT (worker_id) DO UPDATE SET
                name = EXCLUDED.name,
                worker_type = EXCLUDED.worker_type,
                status = EXCLUDED.status,
                capabilities = EXCLUDED.capabilities,
                max_concurrent_tasks = EXCLUDED.max_concurrent_tasks,
                metadata = EXCLUDED.metadata,
                tags = EXCLUDED.tags,
                host_address = EXCLUDED.host_address,
                port = EXCLUDED.port,
                last_heartbeat = NOW(),
                health_status = EXCLUDED.health_status,
                updated_at = NOW()
            "#,
        )
        .bind(params.0)
        .bind(params.1)
        .bind(params.2)
        .bind(params.3)
        .bind(params.4)
        .bind(params.5)
        .bind(params.6)
        .bind(params.7)
        .bind(params.8)
        .bind(params.9)
        .bind(params.10)
        .bind(params.11)
        .bind(params.12)
        .bind(params.13)
        .bind(params.14)
        .bind(params.15)
        .bind(params.16)
        .bind(params.17)
        .bind(params.18)
        .execute(&self.pool)
        .await
        .map_err(|e| {
            orcher_common::error::OrcherError::internal(format!("Failed to upsert worker: {}", e))
        })?;

        info!("Worker upserted: worker_id={}", entity.worker_id);
        Ok(())
    }

    /// Age out workers that have stopped reporting.
    ///
    /// A worker that misses heartbeats is degraded; one that has been silent
    /// long enough is offline. Without this a dead pod stayed `idle` and
    /// `healthy` in the list for ever, because the only stale-worker cleanup
    /// the engine had dropped workers from an in-memory map and never touched
    /// these rows.
    ///
    /// Returns (degraded, offline) row counts.
    pub async fn age_out_silent_workers(
        &self,
        degraded_after_secs: i64,
        offline_after_secs: i64,
    ) -> OrcherResult<(u64, u64)> {
        let offline = sqlx::query(
            r#"
            UPDATE workers
            SET status = 'offline',
                health_status = 'unhealthy',
                -- Whatever it was running, it is not running it now. Keeping
                -- the last reported count left an offline worker advertising
                -- work it cannot be doing.
                current_task_count = 0,
                updated_at = NOW()
            WHERE last_heartbeat < NOW() - make_interval(secs => $1::double precision)
              AND (status <> 'offline' OR health_status <> 'unhealthy' OR current_task_count <> 0)
            "#,
        )
        .bind(offline_after_secs as f64)
        .execute(&self.pool)
        .await
        .map_err(|e| {
            orcher_common::error::OrcherError::internal(format!(
                "Failed to mark silent workers offline: {}",
                e
            ))
        })?;

        let degraded = sqlx::query(
            r#"
            UPDATE workers
            SET health_status = 'degraded', updated_at = NOW()
            WHERE last_heartbeat < NOW() - make_interval(secs => $1::double precision)
              AND last_heartbeat >= NOW() - make_interval(secs => $2::double precision)
              AND health_status <> 'degraded'
            "#,
        )
        .bind(degraded_after_secs as f64)
        .bind(offline_after_secs as f64)
        .execute(&self.pool)
        .await
        .map_err(|e| {
            orcher_common::error::OrcherError::internal(format!(
                "Failed to mark silent workers degraded: {}",
                e
            ))
        })?;

        Ok((degraded.rows_affected(), offline.rows_affected()))
    }

    /// Update worker heartbeat and status by business worker_id string
    ///
    /// Used by the gRPC heartbeat handler which knows the service_id (worker_id),
    /// not the internal UUID.
    pub async fn update_heartbeat_by_worker_id(
        &self,
        worker_id: &str,
        status: &str,
        current_task_count: i32,
    ) -> OrcherResult<()> {
        debug!(
            "Updating heartbeat by worker_id: {}, status: {}",
            worker_id, status
        );

        let result = sqlx::query(
            r#"
            UPDATE workers
            SET last_heartbeat = NOW(),
                status = $1,
                current_task_count = $2,
                -- A heartbeat is the worker saying it is alive. Health was
                -- left at its registration value of 'unknown' forever,
                -- because nothing ever wrote it.
                health_status = 'healthy',
                updated_at = NOW()
            WHERE worker_id = $3
            "#,
        )
        .bind(status)
        .bind(current_task_count)
        .bind(worker_id)
        .execute(&self.pool)
        .await
        .map_err(|e| {
            orcher_common::error::OrcherError::internal(format!(
                "Failed to update heartbeat by worker_id: {}",
                e
            ))
        })?;

        if result.rows_affected() == 0 {
            debug!(
                worker_id = worker_id,
                "No worker found for heartbeat update (may not be persisted yet)"
            );
        }

        Ok(())
    }

    /// Set worker status to offline by business worker_id string
    pub async fn set_offline_by_worker_id(&self, worker_id: &str) -> OrcherResult<()> {
        debug!("Setting worker offline by worker_id: {}", worker_id);

        sqlx::query(
            r#"
            UPDATE workers
            SET status = 'offline',
                updated_at = NOW()
            WHERE worker_id = $1
            "#,
        )
        .bind(worker_id)
        .execute(&self.pool)
        .await
        .map_err(|e| {
            orcher_common::error::OrcherError::internal(format!(
                "Failed to set worker offline: {}",
                e
            ))
        })?;

        Ok(())
    }

    // ========== ANALYTICS METHODS ==========

    /// Count workers by status
    pub async fn count_by_status(&self) -> OrcherResult<std::collections::HashMap<String, i64>> {
        debug!("Counting workers by status");

        let rows: Vec<(String, i64)> = sqlx::query_as(
            r#"
            SELECT status, COUNT(*) as count
            FROM workers
            GROUP BY status
            "#,
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|e| {
            orcher_common::error::OrcherError::internal(format!("Failed to count by status: {}", e))
        })?;

        Ok(rows.into_iter().collect())
    }

    /// Get total worker count
    pub async fn count_total(&self) -> OrcherResult<i64> {
        debug!("Counting total workers");

        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM workers")
            .fetch_one(&self.pool)
            .await
            .map_err(|e| {
                orcher_common::error::OrcherError::internal(format!(
                    "Failed to count workers: {}",
                    e
                ))
            })?;

        Ok(count)
    }
}

#[async_trait]
impl CrudOperations<Worker, WorkerId> for WorkerRepository {
    async fn create(&self, entity: Worker) -> OrcherResult<Worker> {
        debug!(
            "Creating worker: id={}, worker_id={}",
            entity.id, entity.worker_id
        );

        let params = worker_to_params(&entity);

        sqlx::query(
            r#"
            INSERT INTO workers (
                id, organization_id, worker_id, name, worker_type, status,
                capabilities, max_concurrent_tasks, current_task_count,
                cpu_cores, memory_mb, disk_space_mb,
                metadata, tags,
                host_address, port,
                last_heartbeat, health_status, health_details,
                created_at, updated_at, registered_at
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19,
                NOW(), NOW(), NOW()
            )
            "#,
        )
        .bind(params.0)
        .bind(params.1)
        .bind(params.2)
        .bind(params.3)
        .bind(params.4)
        .bind(params.5)
        .bind(params.6)
        .bind(params.7)
        .bind(params.8)
        .bind(params.9)
        .bind(params.10)
        .bind(params.11)
        .bind(params.12)
        .bind(params.13)
        .bind(params.14)
        .bind(params.15)
        .bind(params.16)
        .bind(params.17)
        .bind(params.18)
        .execute(&self.pool)
        .await
        .map_err(|e| {
            orcher_common::error::OrcherError::internal(format!("Failed to create worker: {}", e))
        })?;

        info!("Worker created: id={}", entity.id);
        Ok(entity)
    }

    async fn find_by_id(&self, id: WorkerId) -> OrcherResult<Option<Worker>> {
        debug!("Finding worker by id: {}", id);

        let row: Option<WorkerRow> = sqlx::query_as(
            r#"
            SELECT id, organization_id, worker_id, name, worker_type, status,
                   capabilities, max_concurrent_tasks, current_task_count,
                   cpu_cores, memory_mb, disk_space_mb,
                   metadata, tags,
                   host_address, port,
                   last_heartbeat, health_status, health_details,
                   created_at, updated_at, registered_at
            FROM workers
            WHERE id = $1
            "#,
        )
        .bind(id.as_uuid())
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| {
            orcher_common::error::OrcherError::internal(format!("Failed to find worker: {}", e))
        })?;

        row.map(|r| r.try_into()).transpose()
    }

    async fn update(&self, entity: Worker) -> OrcherResult<Worker> {
        debug!("Updating worker: id={}", entity.id);

        let params = worker_to_params(&entity);

        let result = sqlx::query(
            r#"
            UPDATE workers
            SET worker_id = $3,
                name = $4,
                worker_type = $5,
                status = $6,
                capabilities = $7,
                max_concurrent_tasks = $8,
                current_task_count = $9,
                cpu_cores = $10,
                memory_mb = $11,
                disk_space_mb = $12,
                metadata = $13,
                tags = $14,
                host_address = $15,
                port = $16,
                last_heartbeat = $17,
                health_status = $18,
                health_details = $19,
                updated_at = NOW()
            WHERE id = $1 AND organization_id = $2
            "#,
        )
        .bind(params.0)
        .bind(params.1)
        .bind(params.2)
        .bind(params.3)
        .bind(params.4)
        .bind(params.5)
        .bind(params.6)
        .bind(params.7)
        .bind(params.8)
        .bind(params.9)
        .bind(params.10)
        .bind(params.11)
        .bind(params.12)
        .bind(params.13)
        .bind(params.14)
        .bind(params.15)
        .bind(params.16)
        .bind(params.17)
        .bind(params.18)
        .execute(&self.pool)
        .await
        .map_err(|e| {
            orcher_common::error::OrcherError::internal(format!("Failed to update worker: {}", e))
        })?;

        if result.rows_affected() == 0 {
            return Err(orcher_common::error::OrcherError::NotFound {
                resource_type: "Worker".to_string(),
                id: entity.id.to_string(),
            });
        }

        info!("Worker updated: id={}", entity.id);
        Ok(entity)
    }

    async fn delete(&self, id: WorkerId) -> OrcherResult<bool> {
        debug!("Deleting worker: id={}", id);

        let result = sqlx::query("DELETE FROM workers WHERE id = $1")
            .bind(id.as_uuid())
            .execute(&self.pool)
            .await
            .map_err(|e| {
                orcher_common::error::OrcherError::internal(format!(
                    "Failed to delete worker: {}",
                    e
                ))
            })?;

        if result.rows_affected() > 0 {
            info!("Worker deleted: id={}", id);

            Ok(true)
        } else {
            Ok(false)
        }
    }

    async fn exists(&self, id: WorkerId) -> OrcherResult<bool> {
        debug!("Checking if worker exists: id={}", id);

        let result: Option<bool> =
            sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM workers WHERE id = $1)")
                .bind(id.as_uuid())
                .fetch_one(&self.pool)
                .await
                .map_err(|e| {
                    orcher_common::error::OrcherError::internal(format!(
                        "Failed to check worker existence: {}",
                        e
                    ))
                })?;

        Ok(result.unwrap_or(false))
    }
}

#[async_trait]
impl Repository<Worker> for WorkerRepository {
    type Id = WorkerId;

    async fn create_validated(&self, entity: Worker) -> OrcherResult<Worker> {
        CrudOperations::create(self, entity).await
    }

    async fn find_by_id(&self, id: Self::Id) -> OrcherResult<Option<Worker>> {
        CrudOperations::find_by_id(self, id).await
    }

    async fn update_validated(&self, entity: Worker) -> OrcherResult<Worker> {
        CrudOperations::update(self, entity).await
    }

    async fn delete(&self, id: Self::Id) -> OrcherResult<bool> {
        CrudOperations::delete(self, id).await
    }
}

#[async_trait]
impl WorkerRepositoryTrait for WorkerRepository {
    async fn find_by_status(
        &self,
        organization_id: &OrganizationId,
        status: &str,
    ) -> OrcherResult<Vec<Worker>> {
        debug!(
            "Finding workers by status: {} for organization: {}",
            status, organization_id
        );

        let rows: Vec<WorkerRow> = sqlx::query_as(
            r#"
            SELECT id, organization_id, worker_id, name, worker_type, status,
                   capabilities, max_concurrent_tasks, current_task_count,
                   cpu_cores, memory_mb, disk_space_mb,
                   metadata, tags,
                   host_address, port,
                   last_heartbeat, health_status, health_details,
                   created_at, updated_at, registered_at
            FROM workers
            WHERE organization_id = $1 AND status = $2
            ORDER BY created_at DESC
            "#,
        )
        .bind(organization_id.as_uuid())
        .bind(status)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| {
            orcher_common::error::OrcherError::internal(format!(
                "Failed to find workers by status: {}",
                e
            ))
        })?;

        rows.into_iter()
            .map(|r| r.try_into())
            .collect::<Result<Vec<_>, _>>()
    }

    async fn find_active_workers(
        &self,
        organization_id: &OrganizationId,
    ) -> OrcherResult<Vec<Worker>> {
        debug!(
            "Finding active workers for organization: {}",
            organization_id
        );

        let rows: Vec<WorkerRow> = sqlx::query_as(
            r#"
            SELECT id, organization_id, worker_id, name, worker_type, status,
                   capabilities, max_concurrent_tasks, current_task_count,
                   cpu_cores, memory_mb, disk_space_mb,
                   metadata, tags,
                   host_address, port,
                   last_heartbeat, health_status, health_details,
                   created_at, updated_at, registered_at
            FROM workers
            WHERE organization_id = $1
              AND (status = 'idle' OR status = 'busy')
            ORDER BY created_at DESC
            "#,
        )
        .bind(organization_id.as_uuid())
        .fetch_all(&self.pool)
        .await
        .map_err(|e| {
            orcher_common::error::OrcherError::internal(format!(
                "Failed to find active workers: {}",
                e
            ))
        })?;

        rows.into_iter()
            .map(|r| r.try_into())
            .collect::<Result<Vec<_>, _>>()
    }

    async fn update_status(
        &self,
        organization_id: &OrganizationId,
        worker_id: WorkerId,
        status: &str,
    ) -> OrcherResult<()> {
        debug!(
            "Updating worker {} status to {} in organization: {}",
            worker_id, status, organization_id
        );

        let result = sqlx::query(
            "UPDATE workers SET status = $1, updated_at = NOW() WHERE id = $2 AND organization_id = $3",
        )
        .bind(status)
        .bind(worker_id.as_uuid())
        .bind(organization_id.as_uuid())
        .execute(&self.pool)
        .await
        .map_err(|e| {
            orcher_common::error::OrcherError::internal(format!(
                "Failed to update worker status: {}",
                e
            ))
        })?;

        if result.rows_affected() > 0 {
            Ok(())
        } else {
            Err(orcher_common::error::OrcherError::not_found(
                "Worker".to_string(),
                worker_id.to_string(),
            ))
        }
    }

    async fn update_heartbeat(
        &self,
        organization_id: &OrganizationId,
        worker_id: WorkerId,
    ) -> OrcherResult<()> {
        debug!(
            "Updating heartbeat for worker {} in organization: {}",
            worker_id, organization_id
        );

        let result = sqlx::query(
            "UPDATE workers SET last_heartbeat = NOW(), updated_at = NOW() WHERE id = $1 AND organization_id = $2",
        )
        .bind(worker_id.as_uuid())
        .bind(organization_id.as_uuid())
        .execute(&self.pool)
        .await
        .map_err(|e| {
            orcher_common::error::OrcherError::internal(format!(
                "Failed to update worker heartbeat: {}",
                e
            ))
        })?;

        if result.rows_affected() > 0 {
            Ok(())
        } else {
            Err(orcher_common::error::OrcherError::not_found(
                "Worker".to_string(),
                worker_id.to_string(),
            ))
        }
    }

    async fn find_by_capability(
        &self,
        organization_id: &OrganizationId,
        capability: &str,
    ) -> OrcherResult<Vec<Worker>> {
        debug!(
            "Finding workers by capability: {} in organization: {}",
            capability, organization_id
        );

        let rows: Vec<WorkerRow> = sqlx::query_as(
            r#"
            SELECT id, organization_id, worker_id, name, worker_type, status,
                   capabilities, max_concurrent_tasks, current_task_count,
                   cpu_cores, memory_mb, disk_space_mb,
                   metadata, tags,
                   host_address, port,
                   last_heartbeat, health_status, health_details,
                   created_at, updated_at, registered_at
            FROM workers
            WHERE organization_id = $1 AND $2 = ANY(capabilities)
            ORDER BY created_at DESC
            "#,
        )
        .bind(organization_id.as_uuid())
        .bind(capability)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| {
            orcher_common::error::OrcherError::internal(format!(
                "Failed to find workers by capability: {}",
                e
            ))
        })?;

        rows.into_iter()
            .map(|r| r.try_into())
            .collect::<Result<Vec<_>, _>>()
    }

    async fn update_capacity(
        &self,
        organization_id: &OrganizationId,
        worker_id: WorkerId,
        current_load: i32,
        max_capacity: i32,
    ) -> OrcherResult<()> {
        debug!(
            "Updating capacity for worker {} in organization: {}: current={}, max={}",
            worker_id, organization_id, current_load, max_capacity
        );

        let result = sqlx::query(
            r#"
            UPDATE workers
            SET current_task_count = $1, max_concurrent_tasks = $2, updated_at = NOW()
            WHERE id = $3 AND organization_id = $4
            "#,
        )
        .bind(current_load)
        .bind(max_capacity)
        .bind(worker_id.as_uuid())
        .bind(organization_id.as_uuid())
        .execute(&self.pool)
        .await
        .map_err(|e| {
            orcher_common::error::OrcherError::internal(format!(
                "Failed to update worker capacity: {}",
                e
            ))
        })?;

        if result.rows_affected() > 0 {
            Ok(())
        } else {
            Err(orcher_common::error::OrcherError::not_found(
                "Worker".to_string(),
                worker_id.to_string(),
            ))
        }
    }

    async fn find_available_workers(
        &self,
        organization_id: &OrganizationId,
    ) -> OrcherResult<Vec<Worker>> {
        debug!(
            "Finding available workers for organization: {}",
            organization_id
        );

        let rows: Vec<WorkerRow> = sqlx::query_as(
            r#"
            SELECT id, organization_id, worker_id, name, worker_type, status,
                   capabilities, max_concurrent_tasks, current_task_count,
                   cpu_cores, memory_mb, disk_space_mb,
                   metadata, tags,
                   host_address, port,
                   last_heartbeat, health_status, health_details,
                   created_at, updated_at, registered_at
            FROM workers
            WHERE organization_id = $1
              AND (status = 'idle' OR status = 'busy')
              AND current_task_count < max_concurrent_tasks
            ORDER BY current_task_count ASC, created_at ASC
            "#,
        )
        .bind(organization_id.as_uuid())
        .fetch_all(&self.pool)
        .await
        .map_err(|e| {
            orcher_common::error::OrcherError::internal(format!(
                "Failed to find available workers: {}",
                e
            ))
        })?;

        rows.into_iter()
            .map(|r| r.try_into())
            .collect::<Result<Vec<_>, _>>()
    }

    async fn find_stale_workers(
        &self,
        organization_id: &OrganizationId,
        threshold_seconds: i64,
    ) -> OrcherResult<Vec<Worker>> {
        debug!(
            "Finding stale workers (threshold: {}s) for organization: {}",
            threshold_seconds, organization_id
        );

        let rows: Vec<WorkerRow> = sqlx::query_as(
            r#"
            SELECT id, organization_id, worker_id, name, worker_type, status,
                   capabilities, max_concurrent_tasks, current_task_count,
                   cpu_cores, memory_mb, disk_space_mb,
                   metadata, tags,
                   host_address, port,
                   last_heartbeat, health_status, health_details,
                   created_at, updated_at, registered_at
            FROM workers
            WHERE organization_id = $1
              AND last_heartbeat < NOW() - ($2 || ' seconds')::INTERVAL
            ORDER BY last_heartbeat ASC
            "#,
        )
        .bind(organization_id.as_uuid())
        .bind(threshold_seconds)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| {
            orcher_common::error::OrcherError::internal(format!(
                "Failed to find stale workers: {}",
                e
            ))
        })?;

        rows.into_iter()
            .map(|r| r.try_into())
            .collect::<Result<Vec<_>, _>>()
    }

    async fn update_metadata(
        &self,
        organization_id: &OrganizationId,
        worker_id: WorkerId,
        metadata: serde_json::Value,
    ) -> OrcherResult<()> {
        debug!(
            "Updating metadata for worker {} in organization: {}",
            worker_id, organization_id
        );

        let result = sqlx::query(
            "UPDATE workers SET metadata = $1, updated_at = NOW() WHERE id = $2 AND organization_id = $3",
        )
        .bind(metadata)
        .bind(worker_id.as_uuid())
        .bind(organization_id.as_uuid())
        .execute(&self.pool)
        .await
        .map_err(|e| {
            orcher_common::error::OrcherError::internal(format!(
                "Failed to update worker metadata: {}",
                e
            ))
        })?;

        if result.rows_affected() > 0 {
            Ok(())
        } else {
            Err(orcher_common::error::OrcherError::not_found(
                "Worker".to_string(),
                worker_id.to_string(),
            ))
        }
    }

    async fn count_by_status(
        &self,
        organization_id: &OrganizationId,
        status: &str,
    ) -> OrcherResult<u64> {
        debug!(
            "Counting workers by status: {} for organization: {}",
            status, organization_id
        );

        let count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM workers WHERE organization_id = $1 AND status = $2",
        )
        .bind(organization_id.as_uuid())
        .bind(status)
        .fetch_one(&self.pool)
        .await
        .map_err(|e| {
            orcher_common::error::OrcherError::internal(format!("Failed to count workers: {}", e))
        })?;

        Ok(count as u64)
    }

    async fn find_by_tag(
        &self,
        organization_id: &OrganizationId,
        tag: &str,
    ) -> OrcherResult<Vec<Worker>> {
        debug!(
            "Finding workers by tag: {} in organization: {}",
            tag, organization_id
        );

        let rows: Vec<WorkerRow> = sqlx::query_as(
            r#"
            SELECT id, organization_id, worker_id, name, worker_type, status,
                   capabilities, max_concurrent_tasks, current_task_count,
                   cpu_cores, memory_mb, disk_space_mb,
                   metadata, tags,
                   host_address, port,
                   last_heartbeat, health_status, health_details,
                   created_at, updated_at, registered_at
            FROM workers
            WHERE organization_id = $1 AND $2 = ANY(tags)
            ORDER BY created_at DESC
            "#,
        )
        .bind(organization_id.as_uuid())
        .bind(tag)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| {
            orcher_common::error::OrcherError::internal(format!(
                "Failed to find workers by tag: {}",
                e
            ))
        })?;

        rows.into_iter()
            .map(|r| r.try_into())
            .collect::<Result<Vec<_>, _>>()
    }

    async fn deactivate_worker(
        &self,
        organization_id: &OrganizationId,
        worker_id: WorkerId,
    ) -> OrcherResult<()> {
        debug!(
            "Deactivating worker {} in organization: {}",
            worker_id, organization_id
        );

        let result = sqlx::query(
            "UPDATE workers SET status = 'offline', updated_at = NOW() WHERE id = $1 AND organization_id = $2",
        )
        .bind(worker_id.as_uuid())
        .bind(organization_id.as_uuid())
        .execute(&self.pool)
        .await
        .map_err(|e| {
            orcher_common::error::OrcherError::internal(format!(
                "Failed to deactivate worker: {}",
                e
            ))
        })?;

        if result.rows_affected() > 0 {
            Ok(())
        } else {
            Err(orcher_common::error::OrcherError::not_found(
                "Worker".to_string(),
                worker_id.to_string(),
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use orcher_common::types::identifiers::OrganizationId;
    use sqlx::PgPool;

    async fn test_repo() -> WorkerRepository {
        let url = std::env::var("TEST_DATABASE_URL").unwrap_or_else(|_| {
            "postgresql://postgres:postgres@localhost:5432/orcher_test".to_string()
        });
        WorkerRepository::new(
            PgPool::connect(&url)
                .await
                .expect("connect to the test database"),
        )
    }

    async fn a_registered_worker(repo: &WorkerRepository, worker_id: &str) -> Worker {
        let worker = Worker::new(
            OrganizationId::from_uuid(Uuid::new_v4()),
            worker_id.to_string(),
            worker_id.to_string(),
            "unknown".to_string(),
            0,
        );
        repo.upsert(&worker).await.expect("register the worker");
        worker
    }

    async fn row(repo: &WorkerRepository, worker_id: &str) -> (String, String) {
        sqlx::query_as::<_, (String, String)>(
            "SELECT status, health_status FROM workers WHERE worker_id = $1",
        )
        .bind(worker_id)
        .fetch_one(repo.pool())
        .await
        .expect("read the worker row")
    }

    async fn silent_since(repo: &WorkerRepository, worker_id: &str, secs: i64) {
        sqlx::query(
            "UPDATE workers SET last_heartbeat = NOW() - make_interval(secs => $1::double precision) \
             WHERE worker_id = $2",
        )
        .bind(secs as f64)
        .bind(worker_id)
        .execute(repo.pool())
        .await
        .expect("age the heartbeat");
    }

    async fn forget(repo: &WorkerRepository, worker_id: &str) {
        let _ = sqlx::query("DELETE FROM workers WHERE worker_id = $1")
            .bind(worker_id)
            .execute(repo.pool())
            .await;
    }

    /// Health was written once at registration, as `unknown`, and never
    /// again: nothing called the update. A heartbeat is the worker saying it
    /// is alive, so it says so.
    #[tokio::test]
    #[ignore] // Requires database
    async fn a_heartbeat_marks_the_worker_healthy() {
        let repo = test_repo().await;
        let id = format!("hb-health-{}", Uuid::new_v4());
        a_registered_worker(&repo, &id).await;
        assert_eq!(row(&repo, &id).await.1, "unknown");

        repo.update_heartbeat_by_worker_id(&id, "busy", 3)
            .await
            .expect("heartbeat");

        assert_eq!(
            row(&repo, &id).await,
            ("busy".to_string(), "healthy".to_string())
        );
        forget(&repo, &id).await;
    }

    /// A worker that stops reporting goes degraded, then offline. Before
    /// this, a dead pod stayed idle and healthy in the list for ever.
    #[tokio::test]
    #[ignore] // Requires database
    async fn silent_workers_are_aged_out() {
        let repo = test_repo().await;
        let live = format!("live-{}", Uuid::new_v4());
        let late = format!("late-{}", Uuid::new_v4());
        let gone = format!("gone-{}", Uuid::new_v4());
        for id in [&live, &late, &gone] {
            a_registered_worker(&repo, id).await;
            // The one that will go offline was last seen running work.
            let (status, in_flight) = if id == &gone {
                ("busy", 3)
            } else {
                ("idle", 0)
            };
            repo.update_heartbeat_by_worker_id(id, status, in_flight)
                .await
                .expect("heartbeat");
        }
        silent_since(&repo, &late, 120).await;
        silent_since(&repo, &gone, 3600).await;

        let (degraded, offline) = repo
            .age_out_silent_workers(60, 300)
            .await
            .expect("age out silent workers");
        assert!(degraded >= 1 && offline >= 1);

        assert_eq!(
            row(&repo, &live).await,
            ("idle".to_string(), "healthy".to_string())
        );
        assert_eq!(
            row(&repo, &late).await,
            ("idle".to_string(), "degraded".to_string())
        );
        assert_eq!(
            row(&repo, &gone).await,
            ("offline".to_string(), "unhealthy".to_string())
        );
        let gone_tasks: i32 =
            sqlx::query_scalar("SELECT current_task_count FROM workers WHERE worker_id = $1")
                .bind(&gone)
                .fetch_one(repo.pool())
                .await
                .expect("read the task count");
        assert_eq!(gone_tasks, 0, "an offline worker is not running anything");

        // Idempotent: a second pass changes nothing.
        let (d2, o2) = repo
            .age_out_silent_workers(60, 300)
            .await
            .expect("second pass");
        assert_eq!((d2, o2), (0, 0));

        for id in [&live, &late, &gone] {
            forget(&repo, id).await;
        }
    }

    #[test]
    fn test_worker_repository_creation() {
        // Compile-time test to ensure WorkerRepository can be created
        fn _assert_send_sync<T: Send + Sync>() {}
        _assert_send_sync::<WorkerRepository>();
    }

    #[test]
    fn test_worker_row_conversion() {
        let row = WorkerRow {
            id: Uuid::new_v4(),
            organization_id: Uuid::new_v4(),
            worker_id: "worker-001".to_string(),
            name: "Test Worker".to_string(),
            worker_type: "standard".to_string(),
            status: "idle".to_string(),
            capabilities: Some(vec!["docker".to_string()]),
            max_concurrent_tasks: 10,
            current_task_count: 0,
            cpu_cores: Some(4.0),
            memory_mb: Some(8192),
            disk_space_mb: Some(102400),
            metadata: serde_json::json!({}),
            tags: Some(vec!["production".to_string()]),
            host_address: Some("localhost".to_string()),
            port: Some(8080),
            last_heartbeat: None,
            health_status: "unknown".to_string(),
            health_details: serde_json::json!({}),
            created_at: Timestamptz::now(),
            updated_at: Timestamptz::now(),
            registered_at: None,
        };

        let worker: Result<Worker, _> = row.try_into();
        assert!(worker.is_ok());
    }

    #[test]
    fn test_worker_to_params() {
        let org_id = OrganizationId::new_v4();
        let worker = Worker::new(
            org_id,
            "worker-001".to_string(),
            "Test Worker".to_string(),
            "localhost".to_string(),
            8080,
        );

        let params = worker_to_params(&worker);
        assert_eq!(params.0, worker.id.as_uuid());
        assert_eq!(params.1, worker.organization_id.as_uuid());
        assert_eq!(params.2, "worker-001");
        assert_eq!(params.3, "Test Worker");
        assert_eq!(params.4, "standard");
        assert_eq!(params.5, "idle");
    }
}
