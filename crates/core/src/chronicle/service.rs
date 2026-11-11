//! Chronicle query service.

#![allow(missing_docs)]

use sqlx::{PgPool, Row};
use std::sync::Arc;
use tracing::{debug, error, info};

use super::query_builder::ChronicleQueryBuilder;
use super::types::{
    CountWorkflowsRequest, CountWorkflowsResponse, GetWorkflowRequest, GetWorkflowStatsRequest,
    ListWorkflowsRequest, ListWorkflowsResponse, PageCursor, WorkflowInfo, WorkflowStatsResponse,
};
use orcher_common::error::{OrcherError, OrcherResult};

/// Chronicle service for workflow queries
#[derive(Clone)]
pub struct ChronicleService {
    pool: Arc<PgPool>,
}

impl ChronicleService {
    /// Create a new chronicle service
    pub fn new(pool: Arc<PgPool>) -> Self {
        Self { pool }
    }

    /// List workflows with filtering and pagination
    pub async fn list_workflows(
        &self,
        request: ListWorkflowsRequest,
    ) -> OrcherResult<ListWorkflowsResponse> {
        let start = std::time::Instant::now();

        debug!(
            namespace = %request.namespace,
            status = ?request.status,
            page_size = request.effective_page_size(),
            "Listing workflows"
        );

        // Build and execute query
        let query_builder = ChronicleQueryBuilder::new(&request);
        let mut query = query_builder.build_query();

        let rows = query
            .build()
            .fetch_all(self.pool.as_ref())
            .await
            .map_err(|e| {
                error!("Failed to list workflows: {}", e);
                OrcherError::Internal {
                    message: format!("Database query failed: {}", e),
                    source: None,
                }
            })?;

        // Convert rows to WorkflowInfo
        let workflows: Result<Vec<WorkflowInfo>, _> = rows
            .iter()
            .map(|row| {
                Ok(WorkflowInfo {
                    workflow_id: row.try_get("workflow_id")?,
                    workflow_name: row.try_get("workflow_name")?,
                    namespace: row.try_get("namespace")?,
                    status: row.try_get("status")?,
                    started_at: row.try_get("started_at")?,
                    completed_at: row.try_get("completed_at")?,
                    started_by: row.try_get("started_by")?,
                    parent_workflow_id: row.try_get("parent_workflow_id")?,
                    labels: row.try_get("labels")?,
                    tags: row.try_get("tags")?,
                    last_error: row.try_get("last_error")?,
                })
            })
            .collect();

        let workflows = workflows.map_err(|e: sqlx::Error| {
            error!("Failed to parse workflow rows: {}", e);
            OrcherError::Internal {
                message: format!("Failed to parse results: {}", e),
                source: None,
            }
        })?;

        // Generate next page token if results are full
        let next_page_token = if workflows.len() == request.effective_page_size() as usize {
            workflows.last().and_then(|last| {
                let cursor = PageCursor::new(
                    last.started_at,
                    last.workflow_id,
                    request.effective_order_by(),
                );
                cursor.encode().ok()
            })
        } else {
            None
        };

        // Optionally get total count
        let total_count = if request.include_count {
            Some(self.count_workflows_internal(&request).await?)
        } else {
            None
        };

        let duration = start.elapsed();
        info!(
            namespace = %request.namespace,
            count = workflows.len(),
            duration_ms = duration.as_millis(),
            "Listed workflows"
        );

        Ok(ListWorkflowsResponse {
            workflows,
            next_page_token,
            total_count,
        })
    }

    /// Count workflows matching the filter
    pub async fn count_workflows(
        &self,
        request: CountWorkflowsRequest,
    ) -> OrcherResult<CountWorkflowsResponse> {
        debug!(namespace = %request.namespace, "Counting workflows");

        let list_request = ListWorkflowsRequest {
            namespace: request.namespace.clone(),
            status: request.status,
            workflow_name: request.workflow_name.clone(),
            workflow_name_prefix: None,
            started_by: None,
            parent_workflow_id: None,
            started_after: request.started_after,
            started_before: request.started_before,
            completed_after: None,
            completed_before: None,
            labels: request.labels.clone(),
            tags: request.tags.clone(),
            page_size: None,
            page_token: None,
            order_by: None,
            include_count: false,
        };

        let count = self.count_workflows_internal(&list_request).await?;

        info!(namespace = %request.namespace, count = count, "Counted workflows");

        Ok(CountWorkflowsResponse { count })
    }

    /// Get a single workflow by ID
    pub async fn get_workflow(
        &self,
        request: GetWorkflowRequest,
    ) -> OrcherResult<Option<WorkflowInfo>> {
        debug!(
            namespace = %request.namespace,
            workflow_id = %request.workflow_id,
            "Getting workflow"
        );

        let row = sqlx::query(
            r#"
            SELECT workflow_id, workflow_name, namespace, status,
                   started_at, completed_at, started_by, parent_workflow_id,
                   labels, tags, last_error
            FROM workflow_state_projection
            WHERE namespace = $1 AND workflow_id = $2
            "#,
        )
        .bind(&request.namespace)
        .bind(request.workflow_id)
        .fetch_optional(self.pool.as_ref())
        .await
        .map_err(|e| {
            error!("Failed to get workflow {}: {}", request.workflow_id, e);
            OrcherError::Internal {
                message: format!("Database query failed: {}", e),
                source: None,
            }
        })?;

        let workflow = row
            .map(|row| {
                Ok::<WorkflowInfo, sqlx::Error>(WorkflowInfo {
                    workflow_id: row.try_get("workflow_id")?,
                    workflow_name: row.try_get("workflow_name")?,
                    namespace: row.try_get("namespace")?,
                    status: row.try_get("status")?,
                    started_at: row.try_get("started_at")?,
                    completed_at: row.try_get("completed_at")?,
                    started_by: row.try_get("started_by")?,
                    parent_workflow_id: row.try_get("parent_workflow_id")?,
                    labels: row.try_get("labels")?,
                    tags: row.try_get("tags")?,
                    last_error: row.try_get("last_error")?,
                })
            })
            .transpose()
            .map_err(|e| {
                error!("Failed to parse workflow: {}", e);
                OrcherError::Internal {
                    message: format!("Failed to parse workflow: {}", e),
                    source: None,
                }
            })?;

        Ok(workflow)
    }

    /// Get workflow statistics
    pub async fn get_workflow_stats(
        &self,
        request: GetWorkflowStatsRequest,
    ) -> OrcherResult<WorkflowStatsResponse> {
        debug!(namespace = %request.namespace, "Getting workflow stats");

        let mut query = String::from(
            r#"
            SELECT
                namespace,
                COUNT(*) FILTER (WHERE status IN ('pending', 'running', 'completed', 'failed', 'canceled', 'timed_out')) as total_started,
                COUNT(*) FILTER (WHERE status = 'completed') as total_completed,
                COUNT(*) FILTER (WHERE status = 'failed') as total_failed,
                COUNT(*) FILTER (WHERE status = 'canceled') as total_canceled,
                COUNT(*) FILTER (WHERE status = 'running') as currently_running,
                AVG(EXTRACT(EPOCH FROM (completed_at - started_at))) FILTER (WHERE completed_at IS NOT NULL) as avg_duration_seconds,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (completed_at - started_at)))
                    FILTER (WHERE completed_at IS NOT NULL) as p50_duration_seconds,
                PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (completed_at - started_at)))
                    FILTER (WHERE completed_at IS NOT NULL) as p95_duration_seconds,
                PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (completed_at - started_at)))
                    FILTER (WHERE completed_at IS NOT NULL) as p99_duration_seconds
            FROM workflow_state_projection
            WHERE namespace = $1
            "#,
        );

        let mut param_count = 1;

        // Add time range filters if provided
        if request.start_date.is_some() {
            param_count += 1;
            query.push_str(&format!(" AND started_at >= ${}", param_count));
        }

        if request.end_date.is_some() {
            param_count += 1;
            query.push_str(&format!(" AND started_at <= ${}", param_count));
        }

        query.push_str(" GROUP BY namespace");

        let mut sqlx_query = sqlx::query(&query).bind(&request.namespace);

        if let Some(start_date) = request.start_date {
            sqlx_query = sqlx_query.bind(start_date);
        }

        if let Some(end_date) = request.end_date {
            sqlx_query = sqlx_query.bind(end_date);
        }

        let row = sqlx_query
            .fetch_optional(self.pool.as_ref())
            .await
            .map_err(|e| {
                error!("Failed to get workflow stats: {}", e);
                OrcherError::Internal {
                    message: format!("Database query failed: {}", e),
                    source: None,
                }
            })?;

        let stats = if let Some(row) = row {
            WorkflowStatsResponse {
                namespace: row
                    .try_get("namespace")
                    .unwrap_or_else(|_| request.namespace.clone()),
                total_started: row.try_get("total_started").unwrap_or(0),
                total_completed: row.try_get("total_completed").unwrap_or(0),
                total_failed: row.try_get("total_failed").unwrap_or(0),
                total_canceled: row.try_get("total_canceled").unwrap_or(0),
                currently_running: row.try_get("currently_running").unwrap_or(0),
                avg_duration_seconds: row.try_get("avg_duration_seconds").ok(),
                p50_duration_seconds: row.try_get("p50_duration_seconds").ok(),
                p95_duration_seconds: row.try_get("p95_duration_seconds").ok(),
                p99_duration_seconds: row.try_get("p99_duration_seconds").ok(),
            }
        } else {
            // No data found, return zeros
            WorkflowStatsResponse {
                namespace: request.namespace.clone(),
                total_started: 0,
                total_completed: 0,
                total_failed: 0,
                total_canceled: 0,
                currently_running: 0,
                avg_duration_seconds: None,
                p50_duration_seconds: None,
                p95_duration_seconds: None,
                p99_duration_seconds: None,
            }
        };

        info!(
            namespace = %request.namespace,
            total_started = stats.total_started,
            currently_running = stats.currently_running,
            "Got workflow stats"
        );

        Ok(stats)
    }

    /// Internal helper to count workflows
    async fn count_workflows_internal(&self, request: &ListWorkflowsRequest) -> OrcherResult<i64> {
        // Build and execute query
        let query_builder = ChronicleQueryBuilder::new(request);
        let mut query = query_builder.build_count_query();

        let row = query
            .build()
            .fetch_one(self.pool.as_ref())
            .await
            .map_err(|e| {
                error!("Failed to count workflows: {}", e);
                OrcherError::Internal {
                    message: format!("Database query failed: {}", e),
                    source: None,
                }
            })?;

        let count: i64 = row.try_get("count").map_err(|e| {
            error!("Failed to parse count: {}", e);
            OrcherError::Internal {
                message: format!("Failed to parse count: {}", e),
                source: None,
            }
        })?;

        Ok(count)
    }

    /// Get chronicle metrics for monitoring
    pub async fn get_metrics(&self) -> ChronicleMetrics {
        ChronicleMetrics {
            pool_connections: self.pool.size() as u64,
            pool_idle_connections: self.pool.num_idle() as u64,
        }
    }
}

/// Metrics for the chronicle service
#[derive(Debug, Clone)]
pub struct ChronicleMetrics {
    pub pool_connections: u64,
    pub pool_idle_connections: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    async fn create_test_pool() -> PgPool {
        // This would connect to a test database
        // For now, we'll skip actual database tests
        unimplemented!("Test database connection required")
    }

    #[tokio::test]
    #[ignore] // Requires database
    async fn test_list_workflows() {
        let pool = Arc::new(create_test_pool().await);
        let service = ChronicleService::new(pool);

        let request = ListWorkflowsRequest::new("test-namespace".to_string());

        let result = service.list_workflows(request).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    #[ignore] // Requires database
    async fn test_count_workflows() {
        let pool = Arc::new(create_test_pool().await);
        let service = ChronicleService::new(pool);

        let request = CountWorkflowsRequest {
            namespace: "test-namespace".to_string(),
            status: None,
            workflow_name: None,
            started_after: None,
            started_before: None,
            labels: None,
            tags: None,
        };

        let result = service.count_workflows(request).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    #[ignore] // Requires database
    async fn test_get_workflow() {
        let pool = Arc::new(create_test_pool().await);
        let service = ChronicleService::new(pool);

        let request = GetWorkflowRequest {
            namespace: "test-namespace".to_string(),
            workflow_id: Uuid::new_v4(),
        };

        let result = service.get_workflow(request).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    #[ignore] // Requires database
    async fn test_get_workflow_stats() {
        let pool = Arc::new(create_test_pool().await);
        let service = ChronicleService::new(pool);

        let request = GetWorkflowStatsRequest {
            namespace: "test-namespace".to_string(),
            start_date: None,
            end_date: None,
        };

        let result = service.get_workflow_stats(request).await;
        assert!(result.is_ok());
    }

    #[test]
    fn test_chronicle_metrics() {
        let metrics = ChronicleMetrics {
            pool_connections: 10,
            pool_idle_connections: 5,
        };

        assert_eq!(metrics.pool_connections, 10);
        assert_eq!(metrics.pool_idle_connections, 5);
    }
}
