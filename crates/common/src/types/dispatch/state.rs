//! Workflow execution state types used across dispatch crates.

use crate::types::time::Timestamptz;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use uuid::Uuid;

/// Status of a workflow execution.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "postgres", derive(sqlx::Type))]
#[cfg_attr(
    feature = "postgres",
    sqlx(type_name = "VARCHAR", rename_all = "lowercase")
)]
#[serde(rename_all = "lowercase")]
pub enum WorkflowStatus {
    #[default]
    Running,
    Completed,
    Failed,
    Canceled,
    TimedOut,
}

impl std::fmt::Display for WorkflowStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WorkflowStatus::Running => write!(f, "running"),
            WorkflowStatus::Completed => write!(f, "completed"),
            WorkflowStatus::Failed => write!(f, "failed"),
            WorkflowStatus::Canceled => write!(f, "canceled"),
            WorkflowStatus::TimedOut => write!(f, "timed_out"),
        }
    }
}

/// Pending timer metadata.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TimerInfo {
    pub timer_id: String,
    pub fire_at: Timestamptz,
    pub created_at: Timestamptz,
}

impl TimerInfo {
    pub fn new(timer_id: String, fire_at: Timestamptz) -> Self {
        Self {
            timer_id,
            fire_at,
            created_at: Timestamptz::now(),
        }
    }

    pub fn is_expired(&self) -> bool {
        self.fire_at <= Timestamptz::now()
    }
}

/// Pending event gate (workflow waiting for an external event).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct EventGateInfo {
    pub gate_name: String,
    pub created_at: Timestamptz,
    pub timeout_at: Option<Timestamptz>,
}

impl EventGateInfo {
    pub fn new(gate_name: String) -> Self {
        Self {
            gate_name,
            created_at: Timestamptz::now(),
            timeout_at: None,
        }
    }

    pub fn with_timeout(mut self, timeout_at: Timestamptz) -> Self {
        self.timeout_at = Some(timeout_at);
        self
    }

    pub fn is_timed_out(&self) -> bool {
        self.timeout_at
            .map(|timeout| timeout <= Timestamptz::now())
            .unwrap_or(false)
    }
}

/// Pending task metadata.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TaskInfo {
    pub task_id: String,
    pub task_name: String,
    pub scheduled_at: Timestamptz,
    pub timeout_at: Option<Timestamptz>,
}

impl TaskInfo {
    pub fn new(task_id: String, task_name: String) -> Self {
        Self {
            task_id,
            task_name,
            scheduled_at: Timestamptz::now(),
            timeout_at: None,
        }
    }

    pub fn with_timeout(mut self, timeout_at: Timestamptz) -> Self {
        self.timeout_at = Some(timeout_at);
        self
    }

    pub fn is_timed_out(&self) -> bool {
        self.timeout_at
            .map(|timeout| timeout <= Timestamptz::now())
            .unwrap_or(false)
    }
}

/// In-memory cached workflow execution state for hot-path decisions.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowExecutionState {
    pub workflow_id: Uuid,
    pub workflow_name: String,
    pub namespace: String,
    pub status: WorkflowStatus,
    pub started_at: Timestamptz,
    pub completed_at: Option<Timestamptz>,
    pub current_step: Option<String>,
    pub completed_steps: HashSet<String>,
    pub pending_steps: HashSet<String>,
    pub pending_timers: HashMap<String, TimerInfo>,
    pub pending_event_gates: HashMap<String, EventGateInfo>,
    pub pending_tasks: HashMap<String, TaskInfo>,
    pub last_updated: Timestamptz,

    /// Version counter for optimistic locking.
    pub version: i64,

    pub input: Option<serde_json::Value>,
    pub result: Option<serde_json::Value>,
    pub last_error: Option<String>,

    /// Last loaded journal event ID for incremental loading.
    /// Only journal entries with `event_id` greater than this value need replaying.
    #[serde(default)]
    pub last_journal_event_id: Option<i64>,

    /// When this state was cached, used for freshness checks.
    #[serde(default)]
    pub cached_at: Option<Timestamptz>,
}

impl WorkflowExecutionState {
    pub fn new(
        workflow_id: Uuid,
        workflow_name: String,
        namespace: String,
        input: Option<serde_json::Value>,
    ) -> Self {
        Self {
            workflow_id,
            workflow_name,
            namespace,
            status: WorkflowStatus::Running,
            started_at: Timestamptz::now(),
            completed_at: None,
            current_step: None,
            completed_steps: HashSet::new(),
            pending_steps: HashSet::new(),
            pending_timers: HashMap::new(),
            pending_event_gates: HashMap::new(),
            pending_tasks: HashMap::new(),
            last_updated: Timestamptz::now(),
            version: 1,
            input,
            result: None,
            last_error: None,
            last_journal_event_id: None,
            cached_at: Some(Timestamptz::now()),
        }
    }

    pub fn set_last_journal_event_id(&mut self, event_id: i64) {
        self.last_journal_event_id = Some(event_id);
        self.last_updated = Timestamptz::now();
    }

    pub fn get_last_journal_event_id(&self) -> Option<i64> {
        self.last_journal_event_id
    }

    pub fn needs_refresh(&self, max_age: jiff::SignedDuration) -> bool {
        match self.cached_at {
            Some(cached_at) => Timestamptz::now() - cached_at > max_age,
            None => true, // No cached_at means we should refresh
        }
    }

    pub fn mark_cached(&mut self) {
        self.cached_at = Some(Timestamptz::now());
    }

    pub fn is_terminal(&self) -> bool {
        matches!(
            self.status,
            WorkflowStatus::Completed
                | WorkflowStatus::Failed
                | WorkflowStatus::Canceled
                | WorkflowStatus::TimedOut
        )
    }

    pub fn add_timer(&mut self, timer_id: String, fire_at: Timestamptz) {
        self.pending_timers
            .insert(timer_id.clone(), TimerInfo::new(timer_id, fire_at));
        self.last_updated = Timestamptz::now();
        self.version += 1;
    }

    pub fn remove_timer(&mut self, timer_id: &str) -> Option<TimerInfo> {
        let result = self.pending_timers.remove(timer_id);
        if result.is_some() {
            self.last_updated = Timestamptz::now();
            self.version += 1;
        }
        result
    }

    pub fn add_event_gate(&mut self, gate_name: String, timeout_at: Option<Timestamptz>) {
        let mut gate = EventGateInfo::new(gate_name.clone());
        if let Some(timeout) = timeout_at {
            gate = gate.with_timeout(timeout);
        }
        self.pending_event_gates.insert(gate_name, gate);
        self.last_updated = Timestamptz::now();
        self.version += 1;
    }

    pub fn remove_event_gate(&mut self, gate_name: &str) -> Option<EventGateInfo> {
        let result = self.pending_event_gates.remove(gate_name);
        if result.is_some() {
            self.last_updated = Timestamptz::now();
            self.version += 1;
        }
        result
    }

    pub fn add_task(
        &mut self,
        task_id: String,
        task_name: String,
        timeout_at: Option<Timestamptz>,
    ) {
        let mut task = TaskInfo::new(task_id.clone(), task_name);
        if let Some(timeout) = timeout_at {
            task = task.with_timeout(timeout);
        }
        self.pending_tasks.insert(task_id, task);
        self.last_updated = Timestamptz::now();
        self.version += 1;
    }

    pub fn remove_task(&mut self, task_id: &str) -> Option<TaskInfo> {
        let result = self.pending_tasks.remove(task_id);
        if result.is_some() {
            self.last_updated = Timestamptz::now();
            self.version += 1;
        }
        result
    }

    pub fn complete_step(&mut self, step_id: String) {
        self.pending_steps.remove(&step_id);
        self.completed_steps.insert(step_id);
        self.last_updated = Timestamptz::now();
        self.version += 1;
    }

    pub fn complete(&mut self, result: Option<serde_json::Value>) {
        self.status = WorkflowStatus::Completed;
        self.completed_at = Some(Timestamptz::now());
        self.result = result;
        self.last_updated = Timestamptz::now();
        self.version += 1;
    }

    pub fn fail(&mut self, error: String) {
        self.status = WorkflowStatus::Failed;
        self.completed_at = Some(Timestamptz::now());
        self.last_error = Some(error);
        self.last_updated = Timestamptz::now();
        self.version += 1;
    }

    pub fn estimate_memory_size(&self) -> usize {
        let base_size = std::mem::size_of::<Self>();
        let workflow_name_size = self.workflow_name.len();
        let namespace_size = self.namespace.len();
        let current_step_size = self.current_step.as_ref().map(|s| s.len()).unwrap_or(0);
        let completed_steps_size: usize = self.completed_steps.iter().map(|s| s.len()).sum();
        let pending_steps_size: usize = self.pending_steps.iter().map(|s| s.len()).sum();
        let timers_size: usize = self
            .pending_timers
            .iter()
            .map(|(k, v)| k.len() + std::mem::size_of_val(v))
            .sum();
        let gates_size: usize = self
            .pending_event_gates
            .iter()
            .map(|(k, v)| k.len() + v.gate_name.len() + std::mem::size_of_val(v))
            .sum();
        let tasks_size: usize = self
            .pending_tasks
            .iter()
            .map(|(k, v)| k.len() + v.task_name.len() + std::mem::size_of_val(v))
            .sum();
        let input_size = self
            .input
            .as_ref()
            .map(|v| serde_json::to_string(v).map(|s| s.len()).unwrap_or(0))
            .unwrap_or(0);
        let result_size = self
            .result
            .as_ref()
            .map(|v| serde_json::to_string(v).map(|s| s.len()).unwrap_or(0))
            .unwrap_or(0);
        let error_size = self.last_error.as_ref().map(|s| s.len()).unwrap_or(0);

        base_size
            + workflow_name_size
            + namespace_size
            + current_step_size
            + completed_steps_size
            + pending_steps_size
            + timers_size
            + gates_size
            + tasks_size
            + input_size
            + result_size
            + error_size
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_workflow_status_default() {
        assert_eq!(WorkflowStatus::default(), WorkflowStatus::Running);
    }

    #[test]
    fn test_workflow_status_display() {
        assert_eq!(WorkflowStatus::Running.to_string(), "running");
        assert_eq!(WorkflowStatus::Completed.to_string(), "completed");
        assert_eq!(WorkflowStatus::Failed.to_string(), "failed");
        assert_eq!(WorkflowStatus::Canceled.to_string(), "canceled");
        assert_eq!(WorkflowStatus::TimedOut.to_string(), "timed_out");
    }

    #[test]
    fn test_timer_info_expiration() {
        let future = Timestamptz::now() + jiff::SignedDuration::from_secs(60);
        let timer = TimerInfo::new("timer-1".to_string(), future);
        assert!(!timer.is_expired());

        let past = Timestamptz::now() - jiff::SignedDuration::from_secs(60);
        let expired_timer = TimerInfo::new("timer-2".to_string(), past);
        assert!(expired_timer.is_expired());
    }

    #[test]
    fn test_event_gate_timeout() {
        let gate = EventGateInfo::new("test-event".to_string());
        assert!(!gate.is_timed_out());

        let future = Timestamptz::now() + jiff::SignedDuration::from_secs(60);
        let gate_with_timeout = EventGateInfo::new("test-event".to_string()).with_timeout(future);
        assert!(!gate_with_timeout.is_timed_out());

        let past = Timestamptz::now() - jiff::SignedDuration::from_secs(60);
        let timed_out_gate = EventGateInfo::new("test-event".to_string()).with_timeout(past);
        assert!(timed_out_gate.is_timed_out());
    }

    #[test]
    fn test_task_info_timeout() {
        let task = TaskInfo::new("task-1".to_string(), "charge_card".to_string());
        assert!(!task.is_timed_out());

        let future = Timestamptz::now() + jiff::SignedDuration::from_secs(60);
        let task_with_timeout =
            TaskInfo::new("task-1".to_string(), "charge_card".to_string()).with_timeout(future);
        assert!(!task_with_timeout.is_timed_out());

        let past = Timestamptz::now() - jiff::SignedDuration::from_secs(60);
        let timed_out_task =
            TaskInfo::new("task-1".to_string(), "charge_card".to_string()).with_timeout(past);
        assert!(timed_out_task.is_timed_out());
    }

    #[test]
    fn test_workflow_state_creation() {
        let workflow_id = Uuid::new_v4();
        let state = WorkflowExecutionState::new(
            workflow_id,
            "order_workflow".to_string(),
            "production".to_string(),
            Some(serde_json::json!({"order_id": "123"})),
        );

        assert_eq!(state.workflow_id, workflow_id);
        assert_eq!(state.workflow_name, "order_workflow");
        assert_eq!(state.namespace, "production");
        assert_eq!(state.status, WorkflowStatus::Running);
        assert!(!state.is_terminal());
        assert_eq!(state.version, 1);
    }

    #[test]
    fn test_workflow_state_timer_operations() {
        let mut state = WorkflowExecutionState::new(
            Uuid::new_v4(),
            "test_workflow".to_string(),
            "default".to_string(),
            None,
        );

        let fire_at = Timestamptz::now() + jiff::SignedDuration::from_secs(60);
        state.add_timer("timer-1".to_string(), fire_at);
        assert_eq!(state.pending_timers.len(), 1);
        assert_eq!(state.version, 2);

        let removed = state.remove_timer("timer-1");
        assert!(removed.is_some());
        assert_eq!(state.pending_timers.len(), 0);
        assert_eq!(state.version, 3);
    }

    #[test]
    fn test_workflow_state_event_gate_operations() {
        let mut state = WorkflowExecutionState::new(
            Uuid::new_v4(),
            "test_workflow".to_string(),
            "default".to_string(),
            None,
        );

        state.add_event_gate("payment_received".to_string(), None);
        assert_eq!(state.pending_event_gates.len(), 1);
        assert_eq!(state.version, 2);

        let removed = state.remove_event_gate("payment_received");
        assert!(removed.is_some());
        assert_eq!(state.pending_event_gates.len(), 0);
        assert_eq!(state.version, 3);
    }

    #[test]
    fn test_workflow_state_task_operations() {
        let mut state = WorkflowExecutionState::new(
            Uuid::new_v4(),
            "test_workflow".to_string(),
            "default".to_string(),
            None,
        );

        state.add_task("task-1".to_string(), "charge_card".to_string(), None);
        assert_eq!(state.pending_tasks.len(), 1);
        assert_eq!(state.version, 2);

        let removed = state.remove_task("task-1");
        assert!(removed.is_some());
        assert_eq!(state.pending_tasks.len(), 0);
        assert_eq!(state.version, 3);
    }

    #[test]
    fn test_workflow_state_step_completion() {
        let mut state = WorkflowExecutionState::new(
            Uuid::new_v4(),
            "test_workflow".to_string(),
            "default".to_string(),
            None,
        );

        state.pending_steps.insert("step-1".to_string());
        state.complete_step("step-1".to_string());

        assert!(!state.pending_steps.contains("step-1"));
        assert!(state.completed_steps.contains("step-1"));
    }

    #[test]
    fn test_workflow_state_completion() {
        let mut state = WorkflowExecutionState::new(
            Uuid::new_v4(),
            "test_workflow".to_string(),
            "default".to_string(),
            None,
        );

        let result = serde_json::json!({"status": "success"});
        state.complete(Some(result.clone()));

        assert_eq!(state.status, WorkflowStatus::Completed);
        assert!(state.is_terminal());
        assert!(state.completed_at.is_some());
        assert_eq!(state.result, Some(result));
    }

    #[test]
    fn test_workflow_state_failure() {
        let mut state = WorkflowExecutionState::new(
            Uuid::new_v4(),
            "test_workflow".to_string(),
            "default".to_string(),
            None,
        );

        state.fail("Payment failed".to_string());

        assert_eq!(state.status, WorkflowStatus::Failed);
        assert!(state.is_terminal());
        assert!(state.completed_at.is_some());
        assert_eq!(state.last_error, Some("Payment failed".to_string()));
    }

    #[test]
    fn test_workflow_state_memory_estimation() {
        let state = WorkflowExecutionState::new(
            Uuid::new_v4(),
            "order_workflow".to_string(),
            "production".to_string(),
            Some(serde_json::json!({"order_id": "123"})),
        );

        let size = state.estimate_memory_size();
        assert!(size > 0);
        // Should account for base struct size plus dynamic content
        assert!(size > std::mem::size_of::<WorkflowExecutionState>());
    }

    #[test]
    fn test_workflow_state_version_increments() {
        let mut state = WorkflowExecutionState::new(
            Uuid::new_v4(),
            "test_workflow".to_string(),
            "default".to_string(),
            None,
        );

        assert_eq!(state.version, 1);

        state.add_timer("timer-1".to_string(), Timestamptz::now());
        assert_eq!(state.version, 2);

        state.add_event_gate("event-1".to_string(), None);
        assert_eq!(state.version, 3);

        state.add_task("task-1".to_string(), "test_task".to_string(), None);
        assert_eq!(state.version, 4);

        state.complete_step("step-1".to_string());
        assert_eq!(state.version, 5);
    }
}
