//! Worker Handlers
//!
//! This module implements REST API handlers for worker management operations.
//! Workers are queried directly from the database using sqlx.

use crate::error::{Result, ServerError};
use crate::state::AppState;
use axum::{
    extract::{Query, State},
    Json,
};
use serde::{Deserialize, Serialize};
use tracing::debug;
use uuid::Uuid;

/// Query parameters for listing workers
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListWorkersQuery {
    /// Filter by status (idle, busy, draining, offline, error)
    pub status: Option<String>,

    /// Filter by worker type
    pub worker_type: Option<String>,

    /// Filter by health status (healthy, degraded, unhealthy, unknown)
    pub health_status: Option<String>,

    /// Maximum number of results
    #[serde(default = "default_limit")]
    pub limit: i64,

    /// Offset for pagination
    #[serde(default)]
    pub offset: i64,
}

fn default_limit() -> i64 {
    500
}

/// Worker response returned to callers
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkerResponse {
    pub id: Uuid,
    pub worker_id: String,
    pub name: Option<String>,
    pub worker_type: Option<String>,
    pub status: Option<String>,
    pub health_status: Option<String>,
    pub max_concurrent_tasks: Option<i32>,
    pub current_task_count: Option<i32>,
    pub capabilities: Option<Vec<String>>,
    pub tags: Option<Vec<String>>,
    pub host_address: Option<String>,
    pub port: Option<i32>,
    pub cpu_cores: Option<f64>,
    pub memory_mb: Option<i32>,
    pub disk_space_mb: Option<i32>,
    pub metadata: Option<serde_json::Value>,
    pub last_heartbeat: Option<String>,
    pub created_at: Option<String>,
}

/// List of workers with pagination metadata
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ListWorkersResponse {
    pub workers: Vec<WorkerResponse>,
    pub total: i64,
    pub count: usize,
    pub offset: i64,
}

/// Raw row returned from the workers table
struct WorkerRow {
    id: Uuid,
    worker_id: String,
    name: Option<String>,
    worker_type: Option<String>,
    status: Option<String>,
    health_status: Option<String>,
    max_concurrent_tasks: Option<i32>,
    current_task_count: Option<i32>,
    capabilities: Option<Vec<String>>,
    tags: Option<Vec<String>>,
    host_address: Option<String>,
    port: Option<i32>,
    cpu_cores: Option<f64>,
    memory_mb: Option<i32>,
    disk_space_mb: Option<i32>,
    metadata: Option<serde_json::Value>,
    last_heartbeat: Option<orcher_common::types::time::Timestamptz>,
    created_at: Option<orcher_common::types::time::Timestamptz>,
}

impl sqlx::FromRow<'_, sqlx::postgres::PgRow> for WorkerRow {
    fn from_row(row: &sqlx::postgres::PgRow) -> sqlx::Result<Self> {
        use sqlx::Row;
        Ok(WorkerRow {
            id: row.try_get("id")?,
            worker_id: row.try_get("worker_id")?,
            name: row.try_get("name")?,
            worker_type: row.try_get("worker_type")?,
            status: row.try_get("status")?,
            health_status: row.try_get("health_status")?,
            max_concurrent_tasks: row.try_get("max_concurrent_tasks")?,
            current_task_count: row.try_get("current_task_count")?,
            capabilities: row.try_get("capabilities")?,
            tags: row.try_get("tags")?,
            host_address: row.try_get("host_address")?,
            port: row.try_get("port")?,
            cpu_cores: row.try_get("cpu_cores")?,
            memory_mb: row.try_get("memory_mb")?,
            disk_space_mb: row.try_get("disk_space_mb")?,
            metadata: row.try_get("metadata")?,
            last_heartbeat: row.try_get("last_heartbeat")?,
            created_at: row.try_get("created_at")?,
        })
    }
}

fn row_to_response(row: WorkerRow) -> WorkerResponse {
    WorkerResponse {
        id: row.id,
        worker_id: row.worker_id,
        name: row.name,
        worker_type: row.worker_type,
        status: row.status,
        health_status: row.health_status,
        max_concurrent_tasks: row.max_concurrent_tasks,
        current_task_count: row.current_task_count,
        capabilities: row.capabilities,
        tags: row.tags,
        host_address: row.host_address,
        port: row.port,
        cpu_cores: row.cpu_cores,
        memory_mb: row.memory_mb,
        disk_space_mb: row.disk_space_mb,
        metadata: row.metadata,
        last_heartbeat: row.last_heartbeat.map(|t| t.to_string()),
        created_at: row.created_at.map(|t| t.to_string()),
    }
}

/// List workers
///
/// # Request
///
/// GET /api/v1/workers?limit=100&offset=0
///
/// # Response
///
/// ```json
/// {
///   "workers": [{
///     "id": "550e8400-e29b-41d4-a716-446655440000",
///     "workerId": "worker-abc123",
///     "name": "task-worker-1",
///     "workerType": "task",
///     "status": "idle",
///     "healthStatus": "healthy",
///     "maxConcurrentTasks": 10,
///     "currentTaskCount": 0,
///     "capabilities": ["task-type-a"],
///     "tags": ["production"],
///     "hostAddress": "10.0.0.1",
///     "port": 7233,
///     "lastHeartbeat": "2024-01-15T10:30:00Z",
///     "createdAt": "2024-01-01T00:00:00Z"
///   }],
///   "total": 1,
///   "count": 1,
///   "offset": 0
/// }
/// ```
pub async fn list_workers(
    State(state): State<AppState>,
    Query(mut query): Query<ListWorkersQuery>,
) -> Result<Json<ListWorkersResponse>> {
    // Clamp client-supplied pagination to a safe range (see MAX_PAGE_SIZE).
    query.limit = query.limit.clamp(1, super::MAX_PAGE_SIZE);
    query.offset = query.offset.max(0);

    debug!(
        limit = query.limit,
        offset = query.offset,
        status = ?query.status,
        worker_type = ?query.worker_type,
        health_status = ?query.health_status,
        "Listing workers"
    );

    let pool = state.pool();

    // Only this caller's workers.
    //
    // The gateway resolves the calling organisation and runs the request inside
    // it, and every worker row carries the organisation it registered under —
    // but this query ignored both and returned the fleet of every tenant on the
    // deployment to whoever asked. Names, hosts, ports and queue depths, all of
    // it, to any authenticated caller.
    //
    // None means nobody claimed an organisation, which is a single-tenant
    // deployment: there is nothing to scope to and filtering on NULL would
    // return nothing at all. Same rule the namespace check uses.
    let organization_id = orcher_core::grpc::tenant::current_organization();

    let (total, rows) = fetch_workers(pool, &query, organization_id).await?;

    let workers: Vec<WorkerResponse> = rows.into_iter().map(row_to_response).collect();
    let count = workers.len();

    Ok(Json(ListWorkersResponse {
        workers,
        total,
        count,
        offset: query.offset,
    }))
}

/// The listing itself, split out so the tenant scoping can be tested against a
/// database without standing up the whole HTTP surface.
async fn fetch_workers(
    pool: &sqlx::PgPool,
    query: &ListWorkersQuery,
    organization_id: Option<uuid::Uuid>,
) -> Result<(i64, Vec<WorkerRow>)> {
    // Count total matching workers
    let total: i64 = sqlx::query_scalar(
        r#"
        SELECT COUNT(*)
        FROM workers
        WHERE ($1::text IS NULL OR status = $1)
          AND ($2::text IS NULL OR worker_type = $2)
          AND ($3::text IS NULL OR health_status = $3)
          AND ($4::uuid IS NULL OR organization_id = $4)
        "#,
    )
    .bind(&query.status)
    .bind(&query.worker_type)
    .bind(&query.health_status)
    .bind(organization_id)
    .fetch_one(pool)
    .await
    .map_err(|e| ServerError::internal(format!("Failed to count workers: {}", e)))?;

    // Fetch paginated workers
    let rows = sqlx::query_as::<_, WorkerRow>(
        r#"
        SELECT
            id,
            worker_id,
            name,
            worker_type,
            status,
            health_status,
            max_concurrent_tasks,
            current_task_count,
            capabilities,
            tags,
            host_address,
            port,
            cpu_cores::float8 as cpu_cores,
            memory_mb,
            disk_space_mb,
            metadata,
            last_heartbeat,
            created_at
        FROM workers
        WHERE ($1::text IS NULL OR status = $1)
          AND ($2::text IS NULL OR worker_type = $2)
          AND ($3::text IS NULL OR health_status = $3)
          AND ($4::uuid IS NULL OR organization_id = $4)
        ORDER BY last_heartbeat DESC NULLS LAST, created_at DESC
        LIMIT $5 OFFSET $6
        "#,
    )
    .bind(&query.status)
    .bind(&query.worker_type)
    .bind(&query.health_status)
    .bind(organization_id)
    .bind(query.limit)
    .bind(query.offset)
    .fetch_all(pool)
    .await
    .map_err(|e| ServerError::internal(format!("Failed to list workers: {}", e)))?;

    Ok((total, rows))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_limit() {
        assert_eq!(default_limit(), 500);
    }

    /// Needs Postgres: `TEST_DATABASE_URL=postgresql://... cargo test -p orcher-server -- --ignored`
    mod tenancy {
        use super::*;
        use sqlx::postgres::PgPoolOptions;
        use uuid::Uuid;

        async fn pool() -> sqlx::PgPool {
            let url = std::env::var("TEST_DATABASE_URL").unwrap_or_else(|_| {
                "postgresql://postgres:postgres@localhost:5432/orcher_test".to_string()
            });
            PgPoolOptions::new()
                .max_connections(2)
                .connect(&url)
                .await
                .expect("a database to test against")
        }

        async fn a_worker_of(pool: &sqlx::PgPool, organization_id: Uuid) -> String {
            let worker_id = format!("worker-{}", Uuid::new_v4());
            sqlx::query(
                "INSERT INTO workers (id, worker_id, name, worker_type, status, health_status, \
                 max_concurrent_tasks, current_task_count, organization_id) \
                 VALUES ($1, $2, $2, 'task', 'idle', 'healthy', 1, 0, $3)",
            )
            .bind(Uuid::new_v4())
            .bind(&worker_id)
            .bind(organization_id)
            .execute(pool)
            .await
            .expect("insert a worker");
            worker_id
        }

        fn everything() -> ListWorkersQuery {
            ListWorkersQuery {
                limit: 500,
                offset: 0,
                status: None,
                worker_type: None,
                health_status: None,
            }
        }

        /// The leak: every tenant's fleet — names, hosts, ports — was returned
        /// to whoever asked, because the listing ignored the calling
        /// organisation the gateway had already resolved.
        #[tokio::test]
        #[ignore]
        async fn a_caller_sees_only_its_own_organisations_workers() {
            let pool = pool().await;
            let mine = Uuid::new_v4();
            let theirs = Uuid::new_v4();
            let my_worker = a_worker_of(&pool, mine).await;
            let their_worker = a_worker_of(&pool, theirs).await;

            let (total, rows) = fetch_workers(&pool, &everything(), Some(mine))
                .await
                .expect("the listing");

            let ids: Vec<&str> = rows.iter().map(|r| r.worker_id.as_str()).collect();
            assert!(ids.contains(&my_worker.as_str()), "my own worker is listed");
            assert!(
                !ids.contains(&their_worker.as_str()),
                "another organisation's worker must not be listed"
            );
            assert_eq!(
                total,
                rows.len() as i64,
                "the count matches what is returned"
            );
        }

        /// A deployment with no organisations has nothing to scope to, and
        /// filtering on NULL would hand back an empty list instead.
        #[tokio::test]
        #[ignore]
        async fn without_an_organisation_the_listing_is_unscoped() {
            let pool = pool().await;
            let worker = a_worker_of(&pool, Uuid::new_v4()).await;

            let (_, rows) = fetch_workers(&pool, &everything(), None)
                .await
                .expect("the listing");

            assert!(
                rows.iter().any(|r| r.worker_id == worker),
                "an unscoped caller still sees the fleet"
            );
        }
    }
}
