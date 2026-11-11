//! Task state projection.

use async_trait::async_trait;
use dashmap::DashMap;
use orcher_common::types::identifiers::{AggregateId, TaskId, WorkerId};
use orcher_common::types::time::Timestamptz;
use orcher_common::OrcherResult;
use orcher_storage::schema::types::event::StoredEvent;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Task state information stored in the projection
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct TaskState {
    /// Task identifier
    pub task_id: TaskId,
    /// Current status
    pub status: TaskStatus,
    /// Worker assigned to execute the task
    pub worker_id: Option<WorkerId>,
    /// Submission timestamp
    pub submitted_at: Timestamptz,
    /// Start timestamp (if started)
    pub started_at: Option<Timestamptz>,
    /// Completion/failure timestamp
    pub completed_at: Option<Timestamptz>,
    /// Execution duration in milliseconds
    /// Duration in milliseconds (if completed)
    pub duration_ms: Option<u64>,
    /// Task output (if completed successfully)
    pub output: Option<serde_json::Value>,
    /// Error message (if failed)
    pub error: Option<String>,
    /// Number of retry attempts
    pub retry_count: u32,
    /// Task priority
    pub priority: TaskPriority,
}

/// Task priority level
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize, Default,
)]
pub enum TaskPriority {
    /// Low priority
    Low,
    /// Normal priority
    #[default]
    Normal,
    /// High priority
    High,
    /// Critical priority
    Critical,
}

/// Task status in the projection
/// Task execution status
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub enum TaskStatus {
    /// Task submitted but not yet accepted
    Submitted,
    /// Task pending assignment
    Pending,
    /// Task accepted and queued
    Accepted,
    /// Task is currently executing
    Running,
    /// Task completed successfully
    Completed,
    /// Task failed with error
    Failed,
    /// Task was cancelled
    Cancelled,
}

/// In-memory projection of task states
#[derive(Clone)]
pub struct TaskStateProjection {
    /// Map of task ID to task state
    states: Arc<DashMap<TaskId, TaskState>>,
    /// Last processed event sequence number
    last_sequence: Arc<RwLock<i64>>,
    /// Projection name
    name: String,
}

impl TaskStateProjection {
    /// Create a new task state projection
    pub fn new() -> Self {
        Self {
            states: Arc::new(DashMap::new()),
            last_sequence: Arc::new(RwLock::new(0)),
            name: "task_state".to_string(),
        }
    }

    /// Get the current state of a task
    pub fn get_task_state(&self, task_id: &TaskId) -> Option<TaskState> {
        self.states.get(task_id).map(|entry| entry.value().clone())
    }

    /// Get all tasks with a specific status
    pub fn get_tasks_by_status(&self, status: TaskStatus) -> Vec<TaskState> {
        self.states
            .iter()
            .filter(|entry| entry.value().status == status)
            .map(|entry| entry.value().clone())
            .collect()
    }

    /// Get the count of tasks by status
    pub fn count_by_status(&self, status: TaskStatus) -> usize {
        self.states
            .iter()
            .filter(|entry| entry.value().status == status)
            .count()
    }

    /// Get total number of tasks in the projection
    pub fn total_tasks(&self) -> usize {
        self.states.len()
    }

    /// Get all tasks assigned to a specific worker
    pub fn get_worker_tasks(&self, worker_id: &WorkerId) -> Vec<TaskState> {
        self.states
            .iter()
            .filter(|entry| entry.value().worker_id.as_ref() == Some(worker_id))
            .map(|entry| entry.value().clone())
            .collect()
    }

    /// Insert a task for testing purposes (bypasses event processing)
    #[cfg(test)]
    pub async fn insert_task_for_test(&self, task: TaskState) {
        self.states.insert(task.task_id, task);
    }

    /// Clear all tasks from the projection (for testing)
    #[cfg(test)]
    pub fn clear(&self) {
        self.states.clear();
    }

    /// Handle TaskSubmitted event
    fn handle_task_submitted(&self, event: &StoredEvent) -> OrcherResult<()> {
        if let Some(AggregateId::Task(task_id)) = event.stream_id.to_aggregate_id() {
            let timestamp = event
                .payload
                .get("timestamp")
                .and_then(|v| v.as_str())
                .and_then(|s| Timestamptz::parse_rfc3339(s).ok())
                .unwrap_or_else(Timestamptz::now);

            let state = TaskState {
                task_id,
                status: TaskStatus::Submitted,
                worker_id: None,
                submitted_at: timestamp,
                started_at: None,
                completed_at: None,
                duration_ms: None,
                output: None,
                error: None,
                retry_count: 0,
                priority: TaskPriority::Normal,
            };

            self.states.insert(task_id, state);
        }
        Ok(())
    }

    /// Handle TaskAccepted event
    fn handle_task_accepted(&self, event: &StoredEvent) -> OrcherResult<()> {
        if let Some(AggregateId::Task(task_id)) = event.stream_id.to_aggregate_id() {
            if let Some(mut entry) = self.states.get_mut(&task_id) {
                entry.status = TaskStatus::Accepted;
            }
        }
        Ok(())
    }

    /// Handle TaskStarted event
    fn handle_task_started(&self, event: &StoredEvent) -> OrcherResult<()> {
        if let Some(AggregateId::Task(task_id)) = event.stream_id.to_aggregate_id() {
            if let Some(mut entry) = self.states.get_mut(&task_id) {
                entry.status = TaskStatus::Running;
                entry.started_at = Some(event.timestamp);

                if let Some(worker_id_str) = event.payload.get("worker_id").and_then(|v| v.as_str())
                {
                    if let Ok(uuid) = uuid::Uuid::parse_str(worker_id_str) {
                        entry.worker_id = Some(WorkerId::from_uuid(uuid));
                    }
                }
            }
        }
        Ok(())
    }

    /// Handle TaskCompleted event
    fn handle_task_completed(&self, event: &StoredEvent) -> OrcherResult<()> {
        if let Some(AggregateId::Task(task_id)) = event.stream_id.to_aggregate_id() {
            if let Some(mut entry) = self.states.get_mut(&task_id) {
                entry.status = TaskStatus::Completed;
                entry.completed_at = Some(event.timestamp);

                if let Some(duration) = event.payload.get("duration_ms").and_then(|v| v.as_u64()) {
                    entry.duration_ms = Some(duration);
                }

                // Extract output from result payload
                if let Some(output) = event
                    .payload
                    .get("result")
                    .and_then(|r| r.get("output"))
                    .cloned()
                {
                    if !output.is_null() {
                        entry.output = Some(output);
                    }
                }
            }
        }
        Ok(())
    }

    /// Handle TaskFailed event
    fn handle_task_failed(&self, event: &StoredEvent) -> OrcherResult<()> {
        if let Some(AggregateId::Task(task_id)) = event.stream_id.to_aggregate_id() {
            if let Some(mut entry) = self.states.get_mut(&task_id) {
                entry.status = TaskStatus::Failed;
                entry.completed_at = Some(event.timestamp);

                if let Some(error_str) = event.payload.get("error").and_then(|v| v.as_str()) {
                    entry.error = Some(error_str.to_string());
                }
            }
        }
        Ok(())
    }
}

impl Default for TaskStateProjection {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl orcher_events::projections::Projection for TaskStateProjection {
    type State = HashMap<TaskId, TaskState>;

    fn name(&self) -> &str {
        &self.name
    }

    async fn project(&mut self, event: &StoredEvent) -> Result<(), Box<dyn std::error::Error>> {
        // Route event to appropriate handler based on event type
        match event.event_type.as_str() {
            "TaskSubmitted" => self.handle_task_submitted(event)?,
            "TaskAccepted" => self.handle_task_accepted(event)?,
            "TaskStarted" => self.handle_task_started(event)?,
            "TaskCompleted" => self.handle_task_completed(event)?,
            "TaskFailed" => self.handle_task_failed(event)?,
            _ => {} // Ignore unknown event types
        }

        // Update last processed sequence
        let mut last_seq = self.last_sequence.write().await;
        *last_seq = event.stream_position;

        Ok(())
    }

    async fn get_state(&self) -> Result<Self::State, Box<dyn std::error::Error>> {
        Ok(self.get_all_states())
    }

    async fn reset(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        self.states.clear();
        let mut last_seq = self.last_sequence.write().await;
        *last_seq = 0;
        Ok(())
    }

    fn handles_event_type(&self, event_type: &str) -> bool {
        matches!(
            event_type,
            "TaskSubmitted" | "TaskAccepted" | "TaskStarted" | "TaskCompleted" | "TaskFailed"
        )
    }

    fn version(&self) -> u32 {
        1
    }

    fn category(&self) -> Option<&str> {
        Some("task")
    }
}

impl TaskStateProjection {
    /// Get the last processed sequence number
    pub fn last_processed_sequence(&self) -> i64 {
        // Return the current value using try_read to avoid blocking
        self.last_sequence.try_read().map(|v| *v).unwrap_or(0)
    }

    /// Set the last processed sequence number
    pub fn set_last_processed_sequence(&mut self, sequence: i64) {
        // Use try_write to avoid blocking in tokio runtime
        if let Ok(mut last_seq) = self.last_sequence.try_write() {
            *last_seq = sequence;
        }
    }

    /// Get the projection name
    pub fn projection_name(&self) -> &str {
        &self.name
    }

    /// Get all task states as a HashMap
    fn get_all_states(&self) -> HashMap<TaskId, TaskState> {
        self.states
            .iter()
            .map(|entry| (*entry.key(), entry.value().clone()))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use orcher_common::types::time::Timestamptz;
    use orcher_events::projections::Projection;

    fn create_test_event(
        task_id: TaskId,
        event_type: &str,
        sequence: i64,
        payload: serde_json::Value,
    ) -> StoredEvent {
        StoredEvent::from_aggregate(
            AggregateId::Task(task_id),
            event_type.to_string(),
            payload,
            sequence,
        )
    }

    #[test]
    fn test_projection_creation() {
        let projection = TaskStateProjection::new();
        assert_eq!(projection.projection_name(), "task_state");
        assert_eq!(projection.last_processed_sequence(), 0);
        assert_eq!(projection.total_tasks(), 0);
    }

    #[tokio::test]
    async fn test_task_submitted_event() {
        let mut projection = TaskStateProjection::new();
        let task_id = TaskId::new_v4();

        let event = create_test_event(
            task_id,
            "TaskSubmitted",
            1,
            serde_json::json!({
                "task_id": task_id.as_uuid(),
                "timestamp": Timestamptz::now().to_rfc3339()
            }),
        );

        projection.project(&event).await.unwrap();

        let state = projection.get_task_state(&task_id).unwrap();
        assert_eq!(state.task_id, task_id);
        assert_eq!(state.status, TaskStatus::Submitted);
        assert!(state.worker_id.is_none());
        assert_eq!(projection.last_processed_sequence(), 1);
    }

    #[tokio::test]
    async fn test_task_lifecycle() {
        let mut projection = TaskStateProjection::new();
        let task_id = TaskId::new_v4();
        let worker_id = WorkerId::new_v4();

        // Submit
        let submitted_event = create_test_event(
            task_id,
            "TaskSubmitted",
            1,
            serde_json::json!({
                "task_id": task_id.as_uuid(),
                "timestamp": Timestamptz::now().to_rfc3339()
            }),
        );
        projection.project(&submitted_event).await.unwrap();

        // Accept
        let accepted_event = create_test_event(
            task_id,
            "TaskAccepted",
            2,
            serde_json::json!({
                "task_id": task_id.as_uuid()
            }),
        );
        projection.project(&accepted_event).await.unwrap();

        let state = projection.get_task_state(&task_id).unwrap();
        assert_eq!(state.status, TaskStatus::Accepted);

        // Start
        let started_event = create_test_event(
            task_id,
            "TaskStarted",
            3,
            serde_json::json!({
                "task_id": task_id.as_uuid(),
                "worker_id": worker_id.as_uuid().to_string()
            }),
        );
        projection.project(&started_event).await.unwrap();

        let state = projection.get_task_state(&task_id).unwrap();
        assert_eq!(state.status, TaskStatus::Running);
        assert_eq!(state.worker_id, Some(worker_id));

        // Complete
        let completed_event = create_test_event(
            task_id,
            "TaskCompleted",
            4,
            serde_json::json!({
                "task_id": task_id.as_uuid(),
                "duration_ms": 1500
            }),
        );
        projection.project(&completed_event).await.unwrap();

        let state = projection.get_task_state(&task_id).unwrap();
        assert_eq!(state.status, TaskStatus::Completed);
        assert_eq!(state.duration_ms, Some(1500));
        assert_eq!(projection.last_processed_sequence(), 4);
    }

    #[tokio::test]
    async fn test_task_failed_event() {
        let mut projection = TaskStateProjection::new();
        let task_id = TaskId::new_v4();

        let submitted_event = create_test_event(
            task_id,
            "TaskSubmitted",
            1,
            serde_json::json!({
                "task_id": task_id.as_uuid(),
                "timestamp": Timestamptz::now().to_rfc3339()
            }),
        );
        projection.project(&submitted_event).await.unwrap();

        let failed_event = create_test_event(
            task_id,
            "TaskFailed",
            2,
            serde_json::json!({
                "task_id": task_id.as_uuid(),
                "error": "Connection timeout"
            }),
        );
        projection.project(&failed_event).await.unwrap();

        let state = projection.get_task_state(&task_id).unwrap();
        assert_eq!(state.status, TaskStatus::Failed);
        assert_eq!(state.error, Some("Connection timeout".to_string()));
    }

    #[tokio::test]
    async fn test_get_tasks_by_status() {
        let mut projection = TaskStateProjection::new();

        // Create multiple tasks
        for i in 0..5 {
            let task_id = TaskId::new_v4();
            let event = create_test_event(
                task_id,
                "TaskSubmitted",
                i + 1,
                serde_json::json!({
                    "task_id": task_id.as_uuid(),
                    "timestamp": Timestamptz::now().to_rfc3339()
                }),
            );
            projection.project(&event).await.unwrap();
        }

        let submitted_tasks = projection.get_tasks_by_status(TaskStatus::Submitted);
        assert_eq!(submitted_tasks.len(), 5);
        assert_eq!(projection.count_by_status(TaskStatus::Submitted), 5);
    }

    #[tokio::test]
    async fn test_rebuild_from_events() {
        let mut projection = TaskStateProjection::new();
        let task_id = TaskId::new_v4();

        let events = vec![
            create_test_event(
                task_id,
                "TaskSubmitted",
                1,
                serde_json::json!({
                    "task_id": task_id.as_uuid(),
                    "timestamp": Timestamptz::now().to_rfc3339()
                }),
            ),
            create_test_event(
                task_id,
                "TaskAccepted",
                2,
                serde_json::json!({
                    "task_id": task_id.as_uuid()
                }),
            ),
            create_test_event(
                task_id,
                "TaskCompleted",
                3,
                serde_json::json!({
                    "task_id": task_id.as_uuid(),
                    "duration_ms": 2000
                }),
            ),
        ];

        // Manually project each event
        for event in events {
            projection.project(&event).await.unwrap();
        }

        let state = projection.get_task_state(&task_id).unwrap();
        assert_eq!(state.status, TaskStatus::Completed);
        assert_eq!(state.duration_ms, Some(2000));
        assert_eq!(projection.last_processed_sequence(), 3);
    }

    #[tokio::test]
    async fn test_multiple_tasks_by_status() {
        let mut projection = TaskStateProjection::new();

        // Create tasks in different states
        let task_ids: Vec<_> = (0..10).map(|_| TaskId::new_v4()).collect();

        // 3 submitted
        for i in 0..3 {
            let event = create_test_event(
                task_ids[i],
                "TaskSubmitted",
                i as i64 + 1,
                serde_json::json!({
                    "task_id": task_ids[i].as_uuid(),
                    "timestamp": Timestamptz::now().to_rfc3339()
                }),
            );
            projection.project(&event).await.unwrap();
        }

        // 4 running (submitted then started)
        for i in 3..7 {
            let submitted = create_test_event(
                task_ids[i],
                "TaskSubmitted",
                (i * 2) as i64 + 1,
                serde_json::json!({
                    "task_id": task_ids[i].as_uuid(),
                    "timestamp": Timestamptz::now().to_rfc3339()
                }),
            );
            projection.project(&submitted).await.unwrap();

            let started = create_test_event(
                task_ids[i],
                "TaskStarted",
                (i * 2 + 1) as i64 + 1,
                serde_json::json!({
                    "task_id": task_ids[i].as_uuid(),
                    "worker_id": WorkerId::new_v4().as_uuid().to_string()
                }),
            );
            projection.project(&started).await.unwrap();
        }

        // 3 completed
        for i in 7..10 {
            let submitted = create_test_event(
                task_ids[i],
                "TaskSubmitted",
                (i * 3) as i64 + 1,
                serde_json::json!({
                    "task_id": task_ids[i].as_uuid(),
                    "timestamp": Timestamptz::now().to_rfc3339()
                }),
            );
            projection.project(&submitted).await.unwrap();

            let completed = create_test_event(
                task_ids[i],
                "TaskCompleted",
                (i * 3 + 1) as i64 + 1,
                serde_json::json!({
                    "task_id": task_ids[i].as_uuid(),
                    "duration_ms": 1000
                }),
            );
            projection.project(&completed).await.unwrap();
        }

        // Verify counts
        assert_eq!(projection.count_by_status(TaskStatus::Submitted), 3);
        assert_eq!(projection.count_by_status(TaskStatus::Running), 4);
        assert_eq!(projection.count_by_status(TaskStatus::Completed), 3);
        assert_eq!(projection.total_tasks(), 10);
    }

    #[tokio::test]
    async fn test_worker_task_assignment() {
        let mut projection = TaskStateProjection::new();
        let worker_id = WorkerId::new_v4();

        // Create 5 tasks, assign 3 to the specific worker
        for i in 0..5 {
            let task_id = TaskId::new_v4();

            let submitted = create_test_event(
                task_id,
                "TaskSubmitted",
                (i * 2) as i64 + 1,
                serde_json::json!({
                    "task_id": task_id.as_uuid(),
                    "timestamp": Timestamptz::now().to_rfc3339()
                }),
            );
            projection.project(&submitted).await.unwrap();

            if i < 3 {
                // Assign to our specific worker
                let started = create_test_event(
                    task_id,
                    "TaskStarted",
                    (i * 2 + 1) as i64 + 1,
                    serde_json::json!({
                        "task_id": task_id.as_uuid(),
                        "worker_id": worker_id.as_uuid().to_string()
                    }),
                );
                projection.project(&started).await.unwrap();
            } else {
                // Assign to different worker
                let started = create_test_event(
                    task_id,
                    "TaskStarted",
                    (i * 2 + 1) as i64 + 1,
                    serde_json::json!({
                        "task_id": task_id.as_uuid(),
                        "worker_id": WorkerId::new_v4().as_uuid().to_string()
                    }),
                );
                projection.project(&started).await.unwrap();
            }
        }

        let worker_tasks = projection.get_worker_tasks(&worker_id);
        assert_eq!(worker_tasks.len(), 3);

        // Verify all returned tasks belong to the worker
        for task in worker_tasks {
            assert_eq!(task.worker_id, Some(worker_id));
        }
    }

    #[tokio::test]
    async fn test_projection_sequence_tracking() {
        let mut projection = TaskStateProjection::new();
        assert_eq!(projection.last_processed_sequence(), 0);

        let task_id = TaskId::new_v4();

        // Process events with increasing sequences
        for seq in 1..=5 {
            let event = create_test_event(
                task_id,
                "TaskSubmitted",
                seq,
                serde_json::json!({
                    "task_id": task_id.as_uuid(),
                    "timestamp": Timestamptz::now().to_rfc3339()
                }),
            );
            projection.project(&event).await.unwrap();
            assert_eq!(projection.last_processed_sequence(), seq);
        }
    }

    #[test]
    fn test_set_last_processed_sequence() {
        let mut projection = TaskStateProjection::new();
        projection.set_last_processed_sequence(42);
        assert_eq!(projection.last_processed_sequence(), 42);
    }

    #[tokio::test]
    async fn test_projection_handles_unknown_events() {
        let mut projection = TaskStateProjection::new();
        let task_id = TaskId::new_v4();

        // Unknown event type should be ignored without error
        let unknown_event =
            create_test_event(task_id, "UnknownEventType", 1, serde_json::json!({}));

        projection.project(&unknown_event).await.unwrap();
        assert_eq!(projection.last_processed_sequence(), 1);
        assert!(projection.get_task_state(&task_id).is_none());
    }

    #[test]
    fn test_task_priority_default() {
        let priority = TaskPriority::default();
        assert_eq!(priority, TaskPriority::Normal);
    }

    #[test]
    fn test_task_status_variants() {
        let statuses = [
            TaskStatus::Submitted,
            TaskStatus::Pending,
            TaskStatus::Accepted,
            TaskStatus::Running,
            TaskStatus::Completed,
            TaskStatus::Failed,
            TaskStatus::Cancelled,
        ];

        // Ensure all status variants are distinct
        for (i, status1) in statuses.iter().enumerate() {
            for (j, status2) in statuses.iter().enumerate() {
                if i == j {
                    assert_eq!(status1, status2);
                } else {
                    assert_ne!(status1, status2);
                }
            }
        }
    }
}
