//! Workflow execution journal storage types.

use orcher_common::types::time::Timestamptz;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JournalEntry {
    pub id: i64,
    pub organization_id: Option<uuid::Uuid>,
    pub workflow_id: String,
    pub execution_id: String,
    /// Monotonically increasing within execution; determines ordering.
    pub event_id: i64,
    /// e.g., "WorkflowExecutionStarted", "ChildWorkflowExecutionCompleted"
    pub event_type: String,
    pub timestamp: Timestamptz,
    pub payload: serde_json::Value,
    /// For schema evolution.
    pub version: i32,
    /// 0 if not task-related.
    pub task_id: i64,
    #[serde(default)]
    pub shard_id: Option<i32>,
    pub created_at: Timestamptz,
}

impl JournalEntry {
    pub fn new(
        workflow_id: String,
        execution_id: String,
        event_id: i64,
        event_type: String,
        payload: serde_json::Value,
    ) -> Self {
        Self {
            id: 0, // Will be set by database
            organization_id: None,
            workflow_id,
            execution_id,
            event_id,
            event_type,
            timestamp: Timestamptz::now(),
            payload,
            version: 0,
            task_id: 0,
            shard_id: None,
            created_at: Timestamptz::now(),
        }
    }

    pub fn new_with_org(
        organization_id: uuid::Uuid,
        workflow_id: String,
        execution_id: String,
        event_id: i64,
        event_type: String,
        payload: serde_json::Value,
    ) -> Self {
        Self {
            id: 0, // Will be set by database
            organization_id: Some(organization_id),
            workflow_id,
            execution_id,
            event_id,
            event_type,
            timestamp: Timestamptz::now(),
            payload,
            version: 0,
            task_id: 0,
            shard_id: None,
            created_at: Timestamptz::now(),
        }
    }

    pub fn new_with_timestamp(
        workflow_id: String,
        execution_id: String,
        event_id: i64,
        event_type: String,
        payload: serde_json::Value,
        timestamp: Timestamptz,
    ) -> Self {
        Self {
            id: 0, // Will be set by database
            organization_id: None,
            workflow_id,
            execution_id,
            event_id,
            event_type,
            timestamp,
            payload,
            version: 0,
            task_id: 0,
            shard_id: None,
            created_at: Timestamptz::now(),
        }
    }

    pub fn with_task_id(mut self, task_id: i64) -> Self {
        self.task_id = task_id;
        self
    }

    pub fn with_version(mut self, version: i32) -> Self {
        self.version = version;
        self
    }

    pub fn with_shard_id(mut self, shard_id: i32) -> Self {
        self.shard_id = Some(shard_id);
        self
    }

    pub fn is_child_workflow_event(&self) -> bool {
        self.event_type.starts_with("ChildWorkflowExecution")
    }

    pub fn is_task_event(&self) -> bool {
        self.event_type.contains("Task") && !self.is_child_workflow_event()
    }

    pub fn is_timer_event(&self) -> bool {
        self.event_type.contains("Timer")
    }
}

#[derive(Debug, Clone)]
pub struct JournalEntryBuilder {
    workflow_id: String,
    execution_id: String,
    event_id: i64,
    event_type: String,
    payload: serde_json::Value,
    timestamp: Option<Timestamptz>,
    version: i32,
    task_id: i64,
}

impl JournalEntryBuilder {
    pub fn new(
        workflow_id: String,
        execution_id: String,
        event_id: i64,
        event_type: String,
    ) -> Self {
        Self {
            workflow_id,
            execution_id,
            event_id,
            event_type,
            payload: serde_json::Value::Object(serde_json::Map::new()),
            timestamp: None,
            version: 0,
            task_id: 0,
        }
    }

    pub fn payload(mut self, payload: serde_json::Value) -> Self {
        self.payload = payload;
        self
    }

    pub fn timestamp(mut self, timestamp: Timestamptz) -> Self {
        self.timestamp = Some(timestamp);
        self
    }

    pub fn version(mut self, version: i32) -> Self {
        self.version = version;
        self
    }

    pub fn task_id(mut self, task_id: i64) -> Self {
        self.task_id = task_id;
        self
    }

    pub fn build(self) -> JournalEntry {
        let timestamp = self.timestamp.unwrap_or_else(Timestamptz::now);
        JournalEntry {
            id: 0,
            organization_id: None,
            workflow_id: self.workflow_id,
            execution_id: self.execution_id,
            event_id: self.event_id,
            event_type: self.event_type,
            timestamp,
            payload: self.payload,
            version: self.version,
            task_id: self.task_id,
            created_at: Timestamptz::now(),
            shard_id: None,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct JournalQueryFilters {
    pub workflow_id: Option<String>,
    pub execution_id: Option<String>,
    pub event_type: Option<String>,
    pub min_event_id: Option<i64>,
    pub max_event_id: Option<i64>,
    pub min_timestamp: Option<Timestamptz>,
    pub max_timestamp: Option<Timestamptz>,
    pub task_id: Option<i64>,
    pub limit: Option<i64>,
    pub offset: Option<i64>,
    pub ascending: bool,
}

impl JournalQueryFilters {
    pub fn for_execution(workflow_id: String, execution_id: String) -> Self {
        Self {
            workflow_id: Some(workflow_id),
            execution_id: Some(execution_id),
            ascending: true,
            ..Default::default()
        }
    }

    pub fn for_event_range(
        workflow_id: String,
        execution_id: String,
        min_event_id: i64,
        max_event_id: i64,
    ) -> Self {
        Self {
            workflow_id: Some(workflow_id),
            execution_id: Some(execution_id),
            min_event_id: Some(min_event_id),
            max_event_id: Some(max_event_id),
            ascending: true,
            ..Default::default()
        }
    }

    pub fn for_event_type(workflow_id: String, execution_id: String, event_type: String) -> Self {
        Self {
            workflow_id: Some(workflow_id),
            execution_id: Some(execution_id),
            event_type: Some(event_type),
            ascending: true,
            ..Default::default()
        }
    }

    pub fn descending(mut self) -> Self {
        self.ascending = false;
        self
    }

    pub fn paginate(mut self, limit: i64, offset: i64) -> Self {
        self.limit = Some(limit);
        self.offset = Some(offset);
        self
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JournalStatistics {
    pub total_entries: i64,
    pub max_event_id: i64,
    pub earliest_timestamp: Option<Timestamptz>,
    pub latest_timestamp: Option<Timestamptz>,
    pub event_type_counts: serde_json::Value,
    pub child_workflow_event_count: i64,
    pub task_event_count: i64,
    pub timer_event_count: i64,
}

impl Default for JournalStatistics {
    fn default() -> Self {
        Self {
            total_entries: 0,
            max_event_id: 0,
            earliest_timestamp: None,
            latest_timestamp: None,
            event_type_counts: serde_json::json!({}),
            child_workflow_event_count: 0,
            task_event_count: 0,
            timer_event_count: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_journal_entry_creation() {
        let entry = JournalEntry::new(
            "wf-123".to_string(),
            "exec-456".to_string(),
            1,
            "WorkflowExecutionStarted".to_string(),
            serde_json::json!({"test": "data"}),
        );

        assert_eq!(entry.workflow_id, "wf-123");
        assert_eq!(entry.execution_id, "exec-456");
        assert_eq!(entry.event_id, 1);
        assert_eq!(entry.event_type, "WorkflowExecutionStarted");
    }

    #[test]
    fn test_journal_entry_builder() {
        let entry = JournalEntryBuilder::new(
            "wf-123".to_string(),
            "exec-456".to_string(),
            1,
            "TaskScheduled".to_string(),
        )
        .payload(serde_json::json!({"task_id": "task-1"}))
        .task_id(123)
        .version(1)
        .build();

        assert_eq!(entry.task_id, 123);
        assert_eq!(entry.version, 1);
    }

    #[test]
    fn test_is_child_workflow_event() {
        let entry = JournalEntry::new(
            "wf-123".to_string(),
            "exec-456".to_string(),
            1,
            "ChildWorkflowExecutionStarted".to_string(),
            serde_json::json!({}),
        );

        assert!(entry.is_child_workflow_event());
        assert!(!entry.is_task_event());
        assert!(!entry.is_timer_event());
    }

    #[test]
    fn test_journal_query_filters() {
        let filters =
            JournalQueryFilters::for_execution("wf-123".to_string(), "exec-456".to_string())
                .paginate(10, 0)
                .descending();

        assert_eq!(filters.workflow_id, Some("wf-123".to_string()));
        assert_eq!(filters.execution_id, Some("exec-456".to_string()));
        assert_eq!(filters.limit, Some(10));
        assert_eq!(filters.offset, Some(0));
        assert!(!filters.ascending);
    }

    #[test]
    fn test_journal_entry_with_methods() {
        let entry = JournalEntry::new(
            "wf-123".to_string(),
            "exec-456".to_string(),
            1,
            "TaskScheduled".to_string(),
            serde_json::json!({}),
        )
        .with_task_id(42)
        .with_version(2);

        assert_eq!(entry.task_id, 42);
        assert_eq!(entry.version, 2);
    }
}
