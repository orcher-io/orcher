//! Projection query interface.

use crate::projections::{
    TaskState, TaskStateProjection, TaskStatus, WorkerState, WorkerStateProjection, WorkerStatus,
};
use orcher_common::types::identifiers::{TaskId, WorkerId};
use orcher_common::OrcherResult;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Query filter for task queries
#[derive(Debug, Clone)]
pub enum QueryFilter {
    /// Filter by task status
    ByStatus(TaskStatus),
    /// Filter by worker assignment
    ByWorker(WorkerId),
    /// Filter by multiple criteria (AND operation)
    And(Vec<QueryFilter>),
    /// Filter by multiple criteria (OR operation)
    Or(Vec<QueryFilter>),
    /// No filter (return all)
    All,
}

/// Builder for constructing query filters
#[derive(Debug, Default)]
pub struct QueryBuilder {
    filters: Vec<QueryFilter>,
}

impl QueryBuilder {
    /// Create a new query builder
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a status filter
    pub fn with_status(mut self, status: TaskStatus) -> Self {
        self.filters.push(QueryFilter::ByStatus(status));
        self
    }

    /// Add a worker filter
    pub fn with_worker(mut self, worker_id: WorkerId) -> Self {
        self.filters.push(QueryFilter::ByWorker(worker_id));
        self
    }

    /// Build the final filter (AND of all added filters)
    pub fn build(self) -> QueryFilter {
        if self.filters.is_empty() {
            QueryFilter::All
        } else if self.filters.len() == 1 {
            self.filters.into_iter().next().unwrap()
        } else {
            QueryFilter::And(self.filters)
        }
    }
}

/// Unified query interface for all projections
///
/// Provides a high-level API for querying task and worker state
/// across all registered projections.
pub struct ProjectionQuery {
    task_projection: Arc<RwLock<TaskStateProjection>>,
    worker_projection: Arc<RwLock<WorkerStateProjection>>,
}

impl Default for ProjectionQuery {
    fn default() -> Self {
        Self::new()
    }
}

impl ProjectionQuery {
    /// Create a new projection query interface
    pub fn new() -> Self {
        Self {
            task_projection: Arc::new(RwLock::new(TaskStateProjection::new())),
            worker_projection: Arc::new(RwLock::new(WorkerStateProjection::new())),
        }
    }

    /// Create a new projection query interface with existing projections
    ///
    /// # Arguments
    /// * `task_projection` - The task state projection
    /// * `worker_projection` - The worker state projection
    pub fn new_with_projections(
        task_projection: TaskStateProjection,
        worker_projection: WorkerStateProjection,
    ) -> Self {
        Self {
            task_projection: Arc::new(RwLock::new(task_projection)),
            worker_projection: Arc::new(RwLock::new(worker_projection)),
        }
    }

    /// Get reference to task projection (for testing)
    pub fn task_projection(&self) -> Option<Arc<RwLock<TaskStateProjection>>> {
        Some(Arc::clone(&self.task_projection))
    }

    /// Get reference to worker projection (for testing)
    pub fn worker_projection(&self) -> Option<Arc<RwLock<WorkerStateProjection>>> {
        Some(Arc::clone(&self.worker_projection))
    }

    /// Query tasks with a filter
    ///
    /// # Arguments
    /// * `filter` - The query filter to apply
    ///
    /// # Returns
    /// Vector of matching task states
    pub async fn query_tasks(&self, filter: QueryFilter) -> OrcherResult<Vec<TaskState>> {
        let all_tasks = self.get_all_tasks().await?;
        Ok(self.apply_task_filter(all_tasks, &filter))
    }

    /// Get a specific task by ID
    ///
    /// # Arguments
    /// * `task_id` - The task ID to lookup
    ///
    /// # Returns
    /// The task state if found
    pub async fn get_task(&self, task_id: &TaskId) -> OrcherResult<Option<TaskState>> {
        let projection = self.task_projection.read().await;
        Ok(projection.get_task_state(task_id))
    }

    /// Get all tasks
    pub async fn get_all_tasks(&self) -> OrcherResult<Vec<TaskState>> {
        let projection = self.task_projection.read().await;
        Ok(self.get_all_tasks_internal(&projection))
    }

    /// Get tasks assigned to a specific worker
    ///
    /// # Arguments
    /// * `worker_id` - The worker ID to filter by
    pub async fn get_worker_tasks(&self, worker_id: &WorkerId) -> OrcherResult<Vec<TaskState>> {
        let projection = self.task_projection.read().await;
        Ok(projection.get_worker_tasks(worker_id))
    }

    /// Get all tasks with a specific status
    ///
    /// # Arguments
    /// * `status` - The status to filter by
    pub async fn get_tasks_by_status(&self, status: TaskStatus) -> OrcherResult<Vec<TaskState>> {
        let projection = self.task_projection.read().await;
        Ok(projection.get_tasks_by_status(status))
    }

    /// Get the count of tasks by status
    ///
    /// # Arguments
    /// * `status` - The status to count
    pub async fn count_tasks_by_status(&self, status: TaskStatus) -> usize {
        let projection = self.task_projection.read().await;
        projection.count_by_status(status)
    }

    /// Get total number of tasks
    pub async fn total_tasks(&self) -> usize {
        let projection = self.task_projection.read().await;
        projection.total_tasks()
    }

    /// Get a specific worker by ID
    ///
    /// # Arguments
    /// * `worker_id` - The worker ID to lookup
    pub async fn get_worker(&self, worker_id: &WorkerId) -> Option<WorkerState> {
        let projection = self.worker_projection.read().await;
        projection.get_worker_state(worker_id)
    }

    /// Get worker state by ID
    ///
    /// # Arguments
    /// * `worker_id` - The worker ID to lookup
    pub async fn get_worker_state(
        &self,
        worker_id: &WorkerId,
    ) -> OrcherResult<Option<WorkerState>> {
        let projection = self.worker_projection.read().await;
        Ok(projection.get_worker_state(worker_id))
    }

    /// Get all workers
    pub async fn get_all_workers(&self) -> OrcherResult<Vec<(WorkerId, WorkerState)>> {
        let projection = self.worker_projection.read().await;
        Ok(projection.get_all_workers())
    }

    /// Query all healthy workers
    pub async fn query_healthy_workers(&self) -> OrcherResult<Vec<WorkerState>> {
        let projection = self.worker_projection.read().await;
        Ok(projection.get_healthy_workers())
    }

    /// Query workers by status
    ///
    /// # Arguments
    /// * `status` - The worker status to filter by
    pub async fn query_workers_by_status(
        &self,
        status: WorkerStatus,
    ) -> OrcherResult<Vec<WorkerState>> {
        let projection = self.worker_projection.read().await;
        Ok(projection.get_workers_by_status(status))
    }

    /// Query workers with available capacity
    ///
    /// # Arguments
    /// * `min_capacity` - Minimum required capacity (0.0 to 1.0)
    pub async fn query_available_workers(
        &self,
        min_capacity: f64,
    ) -> OrcherResult<Vec<WorkerState>> {
        let projection = self.worker_projection.read().await;
        Ok(projection.get_available_workers(min_capacity))
    }

    /// Get total number of workers
    pub async fn total_workers(&self) -> usize {
        let projection = self.worker_projection.read().await;
        projection.total_workers()
    }

    /// Get average load across all online workers
    pub async fn average_worker_load(&self) -> f64 {
        let projection = self.worker_projection.read().await;
        projection.average_load()
    }

    /// Get count of workers by status
    pub async fn count_workers_by_status(&self, status: WorkerStatus) -> usize {
        let projection = self.worker_projection.read().await;
        projection.count_by_status(status)
    }

    // Helper methods

    /// Get all tasks from the projection (internal helper)
    fn get_all_tasks_internal(&self, projection: &TaskStateProjection) -> Vec<TaskState> {
        // Get tasks from all statuses
        let mut tasks = Vec::new();
        for status in [
            TaskStatus::Submitted,
            TaskStatus::Accepted,
            TaskStatus::Running,
            TaskStatus::Completed,
            TaskStatus::Failed,
        ] {
            tasks.extend(projection.get_tasks_by_status(status));
        }
        tasks
    }

    /// Apply a filter to a list of tasks
    #[allow(clippy::only_used_in_recursion)]
    fn apply_task_filter(&self, tasks: Vec<TaskState>, filter: &QueryFilter) -> Vec<TaskState> {
        match filter {
            QueryFilter::All => tasks,
            QueryFilter::ByStatus(status) => {
                tasks.into_iter().filter(|t| t.status == *status).collect()
            }
            QueryFilter::ByWorker(worker_id) => tasks
                .into_iter()
                .filter(|t| t.worker_id.as_ref() == Some(worker_id))
                .collect(),
            QueryFilter::And(filters) => {
                let mut result = tasks;
                for f in filters {
                    result = self.apply_task_filter(result, f);
                }
                result
            }
            QueryFilter::Or(filters) => {
                let mut result = Vec::new();
                for f in filters {
                    let filtered = self.apply_task_filter(tasks.clone(), f);
                    for task in filtered {
                        if !result.iter().any(|t: &TaskState| t.task_id == task.task_id) {
                            result.push(task);
                        }
                    }
                }
                result
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_query_builder() {
        let filter = QueryBuilder::new().with_status(TaskStatus::Running).build();

        match filter {
            QueryFilter::ByStatus(status) => assert_eq!(status, TaskStatus::Running),
            _ => panic!("Expected ByStatus filter"),
        }
    }

    #[test]
    fn test_query_builder_multiple_filters() {
        let filter = QueryBuilder::new()
            .with_status(TaskStatus::Running)
            .with_worker(WorkerId::new_v4())
            .build();

        match filter {
            QueryFilter::And(filters) => assert_eq!(filters.len(), 2),
            _ => panic!("Expected And filter"),
        }
    }

    #[test]
    fn test_query_builder_empty() {
        let filter = QueryBuilder::new().build();

        match filter {
            QueryFilter::All => {} // Expected
            _ => panic!("Expected All filter"),
        }
    }

    #[tokio::test]
    async fn test_projection_query_creation() {
        let query = ProjectionQuery::new();
        assert_eq!(query.total_tasks().await, 0);
        assert_eq!(query.total_workers().await, 0);
    }

    #[tokio::test]
    async fn test_get_task() {
        let query = ProjectionQuery::new();

        let task_id = TaskId::new_v4();
        let result = query.get_task(&task_id).await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_get_worker() {
        let query = ProjectionQuery::new();

        let worker_id = WorkerId::new_v4();
        let result = query.get_worker(&worker_id).await;
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_query_healthy_workers() {
        let query = ProjectionQuery::new();

        let workers = query.query_healthy_workers().await.unwrap();
        assert_eq!(workers.len(), 0);
    }

    #[tokio::test]
    async fn test_count_tasks_by_status() {
        let query = ProjectionQuery::new();

        let count = query.count_tasks_by_status(TaskStatus::Running).await;
        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn test_count_workers_by_status() {
        let query = ProjectionQuery::new();

        let count = query.count_workers_by_status(WorkerStatus::Online).await;
        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn test_average_worker_load() {
        let query = ProjectionQuery::new();

        let avg = query.average_worker_load().await;
        assert_eq!(avg, 0.0);
    }

    #[tokio::test]
    async fn test_query_available_workers() {
        let query = ProjectionQuery::new();

        let workers = query.query_available_workers(0.5).await.unwrap();
        assert_eq!(workers.len(), 0);
    }
}
