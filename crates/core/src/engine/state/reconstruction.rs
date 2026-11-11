//! Workflow state reconstruction from journal.

use crate::engine::state::types::{WorkflowExecutionState, WorkflowStatus};
use orcher_common::error::{OrcherError, OrcherResult};
use orcher_common::types::time::Timestamptz;
use sqlx::PgPool;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, error, info};
use uuid::Uuid;

// ============================================================================
// Event Types for Reconstruction
// ============================================================================

/// Event type for state reconstruction
///
/// Maps from the PascalCase event types stored in `workflow_execution_journal`.
#[derive(Debug, Clone, PartialEq)]
pub enum EventType {
    /// Workflow execution started
    WorkflowStarted,
    /// Workflow execution completed successfully
    WorkflowCompleted,
    /// Workflow execution failed
    WorkflowFailed,
    /// Workflow execution was canceled
    WorkflowCanceled,
    /// Workflow execution timed out
    WorkflowTimedOut,
    /// Workflow step started
    StepStarted,
    /// Workflow step completed
    StepCompleted,
    /// Workflow step failed
    StepFailed,
    /// Timer was scheduled
    TimerScheduled,
    /// Timer fired
    TimerFired,
    /// Timer was canceled
    TimerCanceled,
    /// Event gate was created
    EventGateCreated,
    /// Event gate was triggered
    EventGateTriggered,
    /// Event gate timed out
    EventGateTimedOut,
    /// Task was scheduled
    TaskScheduled,
    /// Task started executing
    TaskStarted,
    /// Task completed successfully
    TaskCompleted,
    /// Task failed
    TaskFailed,
    /// Task timed out
    TaskTimedOut,
    /// Unknown event type
    Unknown(String),
}

impl From<&str> for EventType {
    fn from(s: &str) -> Self {
        match s {
            "WorkflowExecutionStarted" | "WorkflowStarted" | "workflow_started" => {
                EventType::WorkflowStarted
            }
            "WorkflowExecutionCompleted" | "WorkflowCompleted" | "workflow_completed" => {
                EventType::WorkflowCompleted
            }
            "WorkflowExecutionFailed" | "WorkflowFailed" | "workflow_failed" => {
                EventType::WorkflowFailed
            }
            "WorkflowCanceled" | "workflow_canceled" => EventType::WorkflowCanceled,
            "WorkflowTimedOut" | "workflow_timed_out" => EventType::WorkflowTimedOut,
            "StepStarted" | "step_started" => EventType::StepStarted,
            "StepCompleted" | "step_completed" => EventType::StepCompleted,
            "StepFailed" | "step_failed" => EventType::StepFailed,
            "TimerScheduled" | "timer_scheduled" => EventType::TimerScheduled,
            "TimerFired" | "timer_fired" => EventType::TimerFired,
            "TimerCanceled" | "timer_canceled" => EventType::TimerCanceled,
            "EventGateCreated" | "event_gate_created" => EventType::EventGateCreated,
            "EventGateTriggered" | "event_gate_triggered" => EventType::EventGateTriggered,
            "EventGateTimedOut" | "event_gate_timed_out" => EventType::EventGateTimedOut,
            "TaskScheduled" | "task_scheduled" => EventType::TaskScheduled,
            "TaskStarted" | "task_started" => EventType::TaskStarted,
            "TaskCompleted" | "task_completed" => EventType::TaskCompleted,
            "TaskFailed" | "task_failed" => EventType::TaskFailed,
            "TaskTimedOut" | "task_timed_out" => EventType::TaskTimedOut,
            other => EventType::Unknown(other.to_string()),
        }
    }
}

/// Raw journal row from workflow_execution_journal
#[derive(Debug, Clone, sqlx::FromRow)]
struct JournalRow {
    event_id: i64,
    event_type: String,
    payload: serde_json::Value,
    created_at: Timestamptz,
}

/// Reconstructed event used for state replay
#[derive(Debug, Clone)]
pub struct ExecutionEvent {
    /// Sequential event ID from the journal
    pub event_id: i64,
    /// Type of event that occurred
    pub event_type: EventType,
    /// Event attributes/payload
    pub payload: serde_json::Value,
    /// Timestamp when the event was created
    pub created_at: Timestamptz,
}

/// Workflow execution record (from workflow_executions table)
#[derive(Debug, Clone, sqlx::FromRow)]
struct WorkflowExecutionRecord {
    workflow_id: Uuid,
    execution_id: String,
    workflow_name: String,
    namespace: String,
    status: String,
    started_at: Option<Timestamptz>,
    completed_at: Option<Timestamptz>,
    input: Option<serde_json::Value>,
    result: Option<serde_json::Value>,
    last_error: Option<String>,
}

// ============================================================================
// State Reconstructor
// ============================================================================

/// Reconstructs workflow state from event log
pub struct StateReconstructor {
    pool: Arc<PgPool>,
}

impl StateReconstructor {
    /// Create a new state reconstructor
    pub fn new(pool: Arc<PgPool>) -> Self {
        Self { pool }
    }

    /// Reconstruct workflow state from event log
    ///
    /// This loads the workflow execution record and all events, then replays
    /// the events to build the complete in-memory state.
    pub async fn reconstruct(
        &self,
        workflow_id: Uuid,
        pool: &PgPool,
    ) -> OrcherResult<WorkflowExecutionState> {
        let mut conn = pool.acquire().await.map_err(|e| OrcherError::Internal {
            message: format!("Failed to get a connection for reconstruction: {}", e),
            source: None,
        })?;
        self.reconstruct_on(workflow_id, &mut conn).await
    }

    /// [`reconstruct`](Self::reconstruct) on a connection the caller already
    /// holds — a transaction it is in the middle of, typically. Taking a
    /// second connection from the pool while holding the first is how a
    /// busy pool deadlocks: every caller holds one and waits for another.
    pub async fn reconstruct_on(
        &self,
        workflow_id: Uuid,
        conn: &mut sqlx::PgConnection,
    ) -> OrcherResult<WorkflowExecutionState> {
        debug!(
            workflow_id = %workflow_id,
            "Starting state reconstruction"
        );

        // 1. Load workflow execution record (from the workflow's shard pool)
        let record = self.load_workflow_record(workflow_id, &mut *conn).await?;

        // 2. Create initial state from record
        let mut state = self.create_initial_state(&record);

        // 3. Load all journal events (same pool — a workflow's state is co-located)
        let events = self.load_events(&record.execution_id, &mut *conn).await?;

        debug!(
            workflow_id = %workflow_id,
            event_count = events.len(),
            "Loaded events for reconstruction"
        );

        // 4. Replay events to build state
        for event in events {
            if let Err(e) = self.apply_event(&mut state, &event) {
                error!(
                    workflow_id = %workflow_id,
                    event_id = %event.event_id,
                    event_type = ?event.event_type,
                    error = %e,
                    "Failed to apply event during reconstruction"
                );
                // Continue with other events
            }
        }

        info!(
            workflow_id = %workflow_id,
            status = ?state.status,
            version = state.version,
            "State reconstruction completed"
        );

        Ok(state)
    }

    /// Recover all in-flight workflows (non-terminal states)
    ///
    /// Returns a list of workflow IDs that need to be recovered.
    /// Used during coordinator startup to resume interrupted workflows.
    pub async fn recover_in_flight_workflows(&self) -> OrcherResult<Vec<Uuid>> {
        debug!("Finding in-flight workflows for recovery");

        let workflow_ids = sqlx::query_scalar::<_, Uuid>(
            r#"
            SELECT id
            FROM workflow_executions
            WHERE status NOT IN ('completed', 'failed', 'canceled', 'timed_out')
            ORDER BY started_at ASC
            "#,
        )
        .fetch_all(&*self.pool)
        .await
        .map_err(|e| OrcherError::Internal {
            message: format!("Failed to load in-flight workflows: {}", e),
            source: None,
        })?;

        info!(
            count = workflow_ids.len(),
            "Found in-flight workflows for recovery"
        );

        Ok(workflow_ids)
    }

    /// Load workflow execution record
    async fn load_workflow_record(
        &self,
        workflow_id: Uuid,
        conn: &mut sqlx::PgConnection,
    ) -> OrcherResult<WorkflowExecutionRecord> {
        sqlx::query_as::<_, WorkflowExecutionRecord>(
            r#"
            SELECT
                id AS workflow_id,
                execution_id,
                workflow_name,
                namespace,
                status,
                started_at,
                completed_at,
                parameters AS input,
                result,
                error AS last_error
            FROM workflow_executions
            WHERE id = $1
            "#,
        )
        .bind(workflow_id)
        .fetch_optional(conn)
        .await
        .map_err(|e| OrcherError::Internal {
            message: format!("Failed to load workflow record: {}", e),
            source: None,
        })?
        .ok_or_else(|| OrcherError::not_found("Workflow", workflow_id.to_string()))
    }

    /// Load all journal events for a workflow and convert to ExecutionEvents
    async fn load_events(
        &self,
        execution_id: &str,
        conn: &mut sqlx::PgConnection,
    ) -> OrcherResult<Vec<ExecutionEvent>> {
        let rows = sqlx::query_as::<_, JournalRow>(
            r#"
            SELECT
                event_id,
                event_type,
                payload,
                created_at
            FROM workflow_execution_journal
            WHERE execution_id = $1
            ORDER BY event_id ASC
            "#,
        )
        .bind(execution_id)
        .fetch_all(conn)
        .await
        .map_err(|e| OrcherError::Internal {
            message: format!("Failed to load journal events: {}", e),
            source: None,
        })?;

        Ok(rows
            .into_iter()
            .map(|row| ExecutionEvent {
                event_id: row.event_id,
                event_type: EventType::from(row.event_type.as_str()),
                payload: row.payload,
                created_at: row.created_at,
            })
            .collect())
    }

    /// Create initial state from workflow record
    fn create_initial_state(&self, record: &WorkflowExecutionRecord) -> WorkflowExecutionState {
        let status = match record.status.as_str() {
            "completed" => WorkflowStatus::Completed,
            "failed" => WorkflowStatus::Failed,
            "canceled" => WorkflowStatus::Canceled,
            "timed_out" => WorkflowStatus::TimedOut,
            _ => WorkflowStatus::Running,
        };

        WorkflowExecutionState {
            workflow_id: record.workflow_id,
            workflow_name: record.workflow_name.clone(),
            namespace: record.namespace.clone(),
            status,
            started_at: record.started_at.unwrap_or_default(),
            completed_at: record.completed_at,
            current_step: None,
            completed_steps: Default::default(),
            pending_steps: Default::default(),
            pending_timers: HashMap::new(),
            pending_event_gates: HashMap::new(),
            pending_tasks: HashMap::new(),
            last_updated: record
                .completed_at
                .or(record.started_at)
                .unwrap_or_default(),
            version: 1,
            input: record.input.clone(),
            result: record.result.clone(),
            last_error: record.last_error.clone(),
            last_journal_event_id: None,
            cached_at: None,
        }
    }

    /// Apply an event to the state
    fn apply_event(
        &self,
        state: &mut WorkflowExecutionState,
        event: &ExecutionEvent,
    ) -> OrcherResult<()> {
        match event.event_type {
            EventType::WorkflowStarted => {
                // Already initialized from record
            }

            EventType::WorkflowCompleted => {
                if let Some(result) = event.payload.get("result") {
                    state.complete(Some(result.clone()));
                } else {
                    state.complete(None);
                }
            }

            EventType::WorkflowFailed => {
                let error = event
                    .payload
                    .get("error")
                    .and_then(|e| e.as_str())
                    .unwrap_or("Unknown error")
                    .to_string();
                state.fail(error);
            }

            EventType::WorkflowCanceled => {
                state.status = WorkflowStatus::Canceled;
                state.completed_at = Some(event.created_at);
            }

            EventType::WorkflowTimedOut => {
                state.status = WorkflowStatus::TimedOut;
                state.completed_at = Some(event.created_at);
            }

            EventType::StepStarted => {
                if let Some(step_id) = event.payload.get("step_id").and_then(|s| s.as_str()) {
                    state.current_step = Some(step_id.to_string());
                    state.pending_steps.insert(step_id.to_string());
                }
            }

            EventType::StepCompleted => {
                if let Some(step_id) = event.payload.get("step_id").and_then(|s| s.as_str()) {
                    state.complete_step(step_id.to_string());
                    if state.current_step.as_deref() == Some(step_id) {
                        state.current_step = None;
                    }
                }
            }

            EventType::StepFailed => {
                if let Some(step_id) = event.payload.get("step_id").and_then(|s| s.as_str()) {
                    state.pending_steps.remove(step_id);
                    if state.current_step.as_deref() == Some(step_id) {
                        state.current_step = None;
                    }
                }
            }

            EventType::TimerScheduled => {
                if let (Some(timer_id), Some(fire_at_str)) = (
                    event.payload.get("timer_id").and_then(|t| t.as_str()),
                    event.payload.get("fire_at").and_then(|f| f.as_str()),
                ) {
                    if let Ok(fire_at) = fire_at_str.parse::<Timestamptz>() {
                        state.add_timer(timer_id.to_string(), fire_at);
                    }
                }
            }

            EventType::TimerFired | EventType::TimerCanceled => {
                if let Some(timer_id) = event.payload.get("timer_id").and_then(|t| t.as_str()) {
                    state.remove_timer(timer_id);
                }
            }

            EventType::EventGateCreated => {
                if let Some(gate_name) = event.payload.get("gate_name").and_then(|g| g.as_str()) {
                    let timeout_at = event
                        .payload
                        .get("timeout_at")
                        .and_then(|t| t.as_str())
                        .and_then(|s| s.parse::<Timestamptz>().ok());

                    state.add_event_gate(gate_name.to_string(), timeout_at);
                }
            }

            EventType::EventGateTriggered | EventType::EventGateTimedOut => {
                if let Some(gate_name) = event.payload.get("gate_name").and_then(|g| g.as_str()) {
                    state.remove_event_gate(gate_name);
                }
            }

            EventType::TaskScheduled => {
                if let (Some(task_id), Some(task_name)) = (
                    event.payload.get("task_id").and_then(|t| t.as_str()),
                    event.payload.get("task_name").and_then(|n| n.as_str()),
                ) {
                    let timeout_at = event
                        .payload
                        .get("timeout_at")
                        .and_then(|t| t.as_str())
                        .and_then(|s| s.parse::<Timestamptz>().ok());

                    state.add_task(task_id.to_string(), task_name.to_string(), timeout_at);
                }
            }

            EventType::TaskStarted => {
                // Task is still pending, no state change needed
            }

            EventType::TaskCompleted | EventType::TaskFailed | EventType::TaskTimedOut => {
                if let Some(task_id) = event.payload.get("task_id").and_then(|t| t.as_str()) {
                    state.remove_task(task_id);
                }
            }

            EventType::Unknown(ref event_type) => {
                debug!(
                    event_type = %event_type,
                    event_id = event.event_id,
                    "Skipping unknown event type during reconstruction"
                );
            }
        }

        Ok(())
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_event_type_from_str() {
        assert_eq!(
            EventType::from("WorkflowExecutionStarted"),
            EventType::WorkflowStarted
        );
        assert_eq!(
            EventType::from("WorkflowExecutionCompleted"),
            EventType::WorkflowCompleted
        );
        assert_eq!(
            EventType::from("WorkflowExecutionFailed"),
            EventType::WorkflowFailed
        );
        assert_eq!(EventType::from("TaskScheduled"), EventType::TaskScheduled);
        assert_eq!(EventType::from("TaskCompleted"), EventType::TaskCompleted);
        assert_eq!(
            EventType::from("workflow_started"),
            EventType::WorkflowStarted
        );
        assert!(matches!(
            EventType::from("SomethingNew"),
            EventType::Unknown(_)
        ));
    }

    #[tokio::test]
    #[ignore] // Requires database
    async fn test_reconstruct_workflow_state() {
        // Test requires real PostgreSQL connection
        // Implementation would:
        // 1. Create a workflow with events
        // 2. Reconstruct state
        // 3. Verify state matches expected
    }

    #[tokio::test]
    #[ignore] // Requires database
    async fn test_recover_in_flight_workflows() {
        // Test requires real PostgreSQL connection
        // Implementation would:
        // 1. Create several workflows in different states
        // 2. Call recover_in_flight_workflows
        // 3. Verify only non-terminal workflows returned
    }

    #[tokio::test]
    #[ignore] // Requires database
    async fn test_event_replay_order() {
        // Test that events are replayed in correct sequence order
    }

    #[tokio::test]
    #[ignore] // Requires database
    async fn test_partial_state_recovery() {
        // Test handling of incomplete event logs
    }
}
