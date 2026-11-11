//! Child workflow execution storage types.

use orcher_common::types::time::Timestamptz;
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ChildWorkflowState {
    Initiated,
    Started,
    Completed,
    Failed,
    TimedOut,
    Canceled,
    Terminated,
}

impl ChildWorkflowState {
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            ChildWorkflowState::Completed
                | ChildWorkflowState::Failed
                | ChildWorkflowState::TimedOut
                | ChildWorkflowState::Canceled
                | ChildWorkflowState::Terminated
        )
    }

    pub fn is_running(&self) -> bool {
        matches!(
            self,
            ChildWorkflowState::Initiated | ChildWorkflowState::Started
        )
    }

    pub fn is_successful(&self) -> bool {
        matches!(self, ChildWorkflowState::Completed)
    }

    pub fn is_failure(&self) -> bool {
        matches!(
            self,
            ChildWorkflowState::Failed
                | ChildWorkflowState::TimedOut
                | ChildWorkflowState::Canceled
                | ChildWorkflowState::Terminated
        )
    }

    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "INITIATED" => Some(ChildWorkflowState::Initiated),
            "STARTED" => Some(ChildWorkflowState::Started),
            "COMPLETED" => Some(ChildWorkflowState::Completed),
            "FAILED" => Some(ChildWorkflowState::Failed),
            "TIMED_OUT" => Some(ChildWorkflowState::TimedOut),
            "CANCELED" => Some(ChildWorkflowState::Canceled),
            "TERMINATED" => Some(ChildWorkflowState::Terminated),
            _ => None,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            ChildWorkflowState::Initiated => "INITIATED",
            ChildWorkflowState::Started => "STARTED",
            ChildWorkflowState::Completed => "COMPLETED",
            ChildWorkflowState::Failed => "FAILED",
            ChildWorkflowState::TimedOut => "TIMED_OUT",
            ChildWorkflowState::Canceled => "CANCELED",
            ChildWorkflowState::Terminated => "TERMINATED",
        }
    }
}

impl fmt::Display for ChildWorkflowState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Defines child behavior when parent workflow closes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum OrphanPolicy {
    /// Child continues independently
    Abandon,
    /// Cancel child
    Cancel,
    /// Force terminate child immediately
    Terminate,
    /// Parent waits for child to complete
    Wait,
}

impl OrphanPolicy {
    pub fn requires_action(&self) -> bool {
        matches!(self, OrphanPolicy::Cancel | OrphanPolicy::Terminate)
    }

    pub fn blocks_parent_close(&self) -> bool {
        matches!(self, OrphanPolicy::Wait)
    }

    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "ABANDON" => Some(OrphanPolicy::Abandon),
            "REQUEST_CANCEL" => Some(OrphanPolicy::Cancel),
            "TERMINATE" => Some(OrphanPolicy::Terminate),
            "WAIT" => Some(OrphanPolicy::Wait),
            _ => None,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            OrphanPolicy::Abandon => "ABANDON",
            OrphanPolicy::Cancel => "REQUEST_CANCEL",
            OrphanPolicy::Terminate => "TERMINATE",
            OrphanPolicy::Wait => "WAIT",
        }
    }
}

impl fmt::Display for OrphanPolicy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl Default for OrphanPolicy {
    fn default() -> Self {
        OrphanPolicy::Abandon
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChildWorkflowExecution {
    pub id: i64,
    pub parent_workflow_id: String,
    pub parent_execution_id: String,
    /// Journal event_id when child was initiated
    pub parent_initiated_event_id: i64,
    /// Journal event_id when child started
    pub parent_started_event_id: Option<i64>,
    pub child_workflow_id: String,
    pub child_execution_id: String,
    pub child_workflow_type: String,
    pub task_queue: String,
    pub input: serde_json::Value,
    pub orphan_policy: OrphanPolicy,
    pub state: ChildWorkflowState,
    pub result: Option<serde_json::Value>,
    pub failure_reason: Option<String>,
    pub failure_details: Option<serde_json::Value>,
    pub initiated_at: Timestamptz,
    pub started_at: Option<Timestamptz>,
    pub completed_at: Option<Timestamptz>,
    pub created_at: Timestamptz,
    pub updated_at: Timestamptz,
    /// Inherited from parent workflow
    #[serde(default)]
    pub shard_id: Option<i32>,
}

impl ChildWorkflowExecution {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        parent_workflow_id: String,
        parent_execution_id: String,
        parent_initiated_event_id: i64,
        child_workflow_id: String,
        child_execution_id: String,
        child_workflow_type: String,
        task_queue: String,
        input: serde_json::Value,
        orphan_policy: OrphanPolicy,
    ) -> Self {
        let now = Timestamptz::now();
        Self {
            id: 0, // Will be set by database
            parent_workflow_id,
            parent_execution_id,
            parent_initiated_event_id,
            parent_started_event_id: None,
            child_workflow_id,
            child_execution_id,
            child_workflow_type,
            task_queue,
            input,
            orphan_policy,
            state: ChildWorkflowState::Initiated,
            result: None,
            failure_reason: None,
            failure_details: None,
            initiated_at: now,
            started_at: None,
            completed_at: None,
            created_at: now,
            updated_at: now,
            shard_id: None,
        }
    }

    pub fn mark_started(&mut self, parent_started_event_id: i64) {
        self.state = ChildWorkflowState::Started;
        self.parent_started_event_id = Some(parent_started_event_id);
        self.started_at = Some(Timestamptz::now());
        self.updated_at = Timestamptz::now();
    }

    pub fn mark_completed(&mut self, result: serde_json::Value) {
        self.state = ChildWorkflowState::Completed;
        self.result = Some(result);
        self.completed_at = Some(Timestamptz::now());
        self.updated_at = Timestamptz::now();
    }

    pub fn mark_failed(&mut self, reason: String, details: Option<serde_json::Value>) {
        self.state = ChildWorkflowState::Failed;
        self.failure_reason = Some(reason);
        self.failure_details = details;
        self.completed_at = Some(Timestamptz::now());
        self.updated_at = Timestamptz::now();
    }

    pub fn mark_timed_out(&mut self, reason: String) {
        self.state = ChildWorkflowState::TimedOut;
        self.failure_reason = Some(reason);
        self.completed_at = Some(Timestamptz::now());
        self.updated_at = Timestamptz::now();
    }

    pub fn mark_canceled(&mut self, reason: String) {
        self.state = ChildWorkflowState::Canceled;
        self.failure_reason = Some(reason);
        self.completed_at = Some(Timestamptz::now());
        self.updated_at = Timestamptz::now();
    }

    pub fn mark_terminated(&mut self, reason: String) {
        self.state = ChildWorkflowState::Terminated;
        self.failure_reason = Some(reason);
        self.completed_at = Some(Timestamptz::now());
        self.updated_at = Timestamptz::now();
    }

    pub fn is_terminal(&self) -> bool {
        self.state.is_terminal()
    }

    pub fn is_running(&self) -> bool {
        self.state.is_running()
    }

    pub fn duration(&self) -> Option<jiff::SignedDuration> {
        self.completed_at
            .map(|completed| completed - self.initiated_at)
    }

    pub fn execution_duration(&self) -> Option<jiff::SignedDuration> {
        if let (Some(started), Some(completed)) = (self.started_at, self.completed_at) {
            Some(completed - started)
        } else {
            None
        }
    }
}

#[derive(Debug, Clone)]
pub struct ChildWorkflowExecutionBuilder {
    parent_workflow_id: String,
    parent_execution_id: String,
    parent_initiated_event_id: i64,
    child_workflow_id: String,
    child_execution_id: String,
    child_workflow_type: String,
    task_queue: String,
    input: serde_json::Value,
    orphan_policy: OrphanPolicy,
}

impl ChildWorkflowExecutionBuilder {
    pub fn new(
        parent_workflow_id: String,
        parent_execution_id: String,
        parent_initiated_event_id: i64,
        child_workflow_id: String,
        child_execution_id: String,
        child_workflow_type: String,
    ) -> Self {
        Self {
            parent_workflow_id,
            parent_execution_id,
            parent_initiated_event_id,
            child_workflow_id,
            child_execution_id,
            child_workflow_type,
            task_queue: "default".to_string(),
            input: serde_json::json!({}),
            orphan_policy: OrphanPolicy::default(),
        }
    }

    pub fn task_queue(mut self, task_queue: String) -> Self {
        self.task_queue = task_queue;
        self
    }

    pub fn input(mut self, input: serde_json::Value) -> Self {
        self.input = input;
        self
    }

    pub fn orphan_policy(mut self, policy: OrphanPolicy) -> Self {
        self.orphan_policy = policy;
        self
    }

    pub fn build(self) -> ChildWorkflowExecution {
        ChildWorkflowExecution::new(
            self.parent_workflow_id,
            self.parent_execution_id,
            self.parent_initiated_event_id,
            self.child_workflow_id,
            self.child_execution_id,
            self.child_workflow_type,
            self.task_queue,
            self.input,
            self.orphan_policy,
        )
    }
}

#[derive(Debug, Clone, Default)]
pub struct ChildWorkflowQueryFilters {
    pub parent_workflow_id: Option<String>,
    pub parent_execution_id: Option<String>,
    pub child_workflow_id: Option<String>,
    pub child_execution_id: Option<String>,
    pub state: Option<ChildWorkflowState>,
    pub orphan_policy: Option<OrphanPolicy>,
    pub only_running: bool,
    pub only_terminal: bool,
    pub limit: Option<i64>,
    pub offset: Option<i64>,
}

impl ChildWorkflowQueryFilters {
    pub fn for_parent(parent_workflow_id: String, parent_execution_id: String) -> Self {
        Self {
            parent_workflow_id: Some(parent_workflow_id),
            parent_execution_id: Some(parent_execution_id),
            ..Default::default()
        }
    }

    pub fn for_child(child_workflow_id: String, child_execution_id: String) -> Self {
        Self {
            child_workflow_id: Some(child_workflow_id),
            child_execution_id: Some(child_execution_id),
            ..Default::default()
        }
    }

    pub fn only_running(mut self) -> Self {
        self.only_running = true;
        self.only_terminal = false;
        self
    }

    pub fn only_terminal(mut self) -> Self {
        self.only_terminal = true;
        self.only_running = false;
        self
    }

    pub fn paginate(mut self, limit: i64, offset: i64) -> Self {
        self.limit = Some(limit);
        self.offset = Some(offset);
        self
    }

    pub fn with_state(mut self, state: ChildWorkflowState) -> Self {
        self.state = Some(state);
        self
    }

    pub fn with_policy(mut self, policy: OrphanPolicy) -> Self {
        self.orphan_policy = Some(policy);
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_child_workflow_state() {
        assert!(ChildWorkflowState::Completed.is_terminal());
        assert!(ChildWorkflowState::Failed.is_terminal());
        assert!(!ChildWorkflowState::Started.is_terminal());
        assert!(ChildWorkflowState::Started.is_running());
        assert!(ChildWorkflowState::Completed.is_successful());
        assert!(ChildWorkflowState::Failed.is_failure());
    }

    #[test]
    fn test_orphan_policy() {
        assert!(OrphanPolicy::Cancel.requires_action());
        assert!(OrphanPolicy::Terminate.requires_action());
        assert!(!OrphanPolicy::Abandon.requires_action());
        assert!(OrphanPolicy::Wait.blocks_parent_close());
        assert!(!OrphanPolicy::Abandon.blocks_parent_close());
    }

    #[test]
    fn test_child_workflow_creation() {
        let child = ChildWorkflowExecution::new(
            "parent-wf".to_string(),
            "parent-exec".to_string(),
            1,
            "child-wf".to_string(),
            "child-exec".to_string(),
            "ChildWorkflow".to_string(),
            "default".to_string(),
            serde_json::json!({"input": "data"}),
            OrphanPolicy::Wait,
        );

        assert_eq!(child.parent_workflow_id, "parent-wf");
        assert_eq!(child.child_workflow_id, "child-wf");
        assert_eq!(child.state, ChildWorkflowState::Initiated);
        assert!(child.is_running());
        assert!(!child.is_terminal());
    }

    #[test]
    fn test_child_workflow_lifecycle() {
        let mut child = ChildWorkflowExecution::new(
            "parent-wf".to_string(),
            "parent-exec".to_string(),
            1,
            "child-wf".to_string(),
            "child-exec".to_string(),
            "ChildWorkflow".to_string(),
            "default".to_string(),
            serde_json::json!({}),
            OrphanPolicy::Abandon,
        );

        // Start the child
        child.mark_started(2);
        assert_eq!(child.state, ChildWorkflowState::Started);
        assert!(child.started_at.is_some());
        assert_eq!(child.parent_started_event_id, Some(2));

        // Complete the child
        child.mark_completed(serde_json::json!({"result": "success"}));
        assert_eq!(child.state, ChildWorkflowState::Completed);
        assert!(child.result.is_some());
        assert!(child.completed_at.is_some());
        assert!(child.is_terminal());
    }

    #[test]
    fn test_child_workflow_builder() {
        let child = ChildWorkflowExecutionBuilder::new(
            "parent-wf".to_string(),
            "parent-exec".to_string(),
            1,
            "child-wf".to_string(),
            "child-exec".to_string(),
            "ChildWorkflow".to_string(),
        )
        .task_queue("custom-queue".to_string())
        .input(serde_json::json!({"key": "value"}))
        .orphan_policy(OrphanPolicy::Terminate)
        .build();

        assert_eq!(child.task_queue, "custom-queue");
        assert_eq!(child.orphan_policy, OrphanPolicy::Terminate);
    }

    #[test]
    fn test_query_filters() {
        let filters = ChildWorkflowQueryFilters::for_parent(
            "parent-wf".to_string(),
            "parent-exec".to_string(),
        )
        .only_running()
        .paginate(10, 0);

        assert_eq!(filters.parent_workflow_id, Some("parent-wf".to_string()));
        assert!(filters.only_running);
        assert_eq!(filters.limit, Some(10));
    }

    #[test]
    fn test_state_string_conversion() {
        assert_eq!(ChildWorkflowState::Started.as_str(), "STARTED");
        assert_eq!(
            ChildWorkflowState::from_str("COMPLETED"),
            Some(ChildWorkflowState::Completed)
        );
        assert_eq!(ChildWorkflowState::from_str("INVALID"), None);
    }

    #[test]
    fn test_policy_string_conversion() {
        assert_eq!(OrphanPolicy::Wait.as_str(), "WAIT");
        assert_eq!(
            OrphanPolicy::from_str("TERMINATE"),
            Some(OrphanPolicy::Terminate)
        );
        assert_eq!(OrphanPolicy::from_str("INVALID"), None);
    }
}
