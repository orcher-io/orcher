//! Workflow state projection.

use async_trait::async_trait;
use dashmap::DashMap;
use orcher_common::types::identifiers::{AggregateId, WorkflowId};
use orcher_common::types::time::Timestamptz;
use orcher_events::bus::EventBus;
use orcher_events::domain::WorkflowEvent;
use orcher_storage::schema::types::event::StoredEvent;
use std::collections::HashMap;
use std::error::Error;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::task::JoinHandle;
use tracing::{error, info};

/// Workflow status in the projection
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub enum WorkflowStatus {
    /// Workflow is currently running
    Running,
    /// Workflow is paused
    Paused,
    /// Workflow completed successfully
    Completed,
    /// Workflow failed with error
    Failed,
    /// Workflow was cancelled
    Cancelled,
}

/// Workflow state information stored in the projection
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct WorkflowState {
    /// Workflow identifier
    pub workflow_id: WorkflowId,
    /// Current status
    pub status: WorkflowStatus,
    /// When the workflow started
    pub started_at: Timestamptz,
    /// When the workflow completed/failed/cancelled
    pub completed_at: Option<Timestamptz>,
    /// List of completed step IDs
    pub completed_steps: Vec<String>,
    /// Map of failed step IDs to error messages
    pub failed_steps: HashMap<String, String>,
    /// Error message if workflow failed
    pub error: Option<String>,
    /// Last update timestamp
    pub last_updated: Timestamptz,
}

/// In-memory projection of workflow states
#[derive(Clone)]
pub struct WorkflowStateProjection {
    /// Map of workflow ID to workflow state
    states: Arc<DashMap<WorkflowId, WorkflowState>>,
    /// Last processed event sequence number
    last_sequence: Arc<RwLock<i64>>,
    /// Projection name
    name: String,
}

impl WorkflowStateProjection {
    /// Create a new workflow state projection
    pub fn new() -> Self {
        Self {
            states: Arc::new(DashMap::new()),
            last_sequence: Arc::new(RwLock::new(0)),
            name: "workflow_state".to_string(),
        }
    }

    /// Get the current state of a workflow
    pub fn get_workflow_state(&self, workflow_id: &WorkflowId) -> Option<WorkflowState> {
        self.states
            .get(workflow_id)
            .map(|entry| entry.value().clone())
    }

    /// Get all workflows with a specific status
    pub fn get_workflows_by_status(&self, status: WorkflowStatus) -> Vec<WorkflowState> {
        self.states
            .iter()
            .filter(|entry| entry.value().status == status)
            .map(|entry| entry.value().clone())
            .collect()
    }

    /// Get the count of workflows by status
    pub fn count_by_status(&self, status: WorkflowStatus) -> usize {
        self.states
            .iter()
            .filter(|entry| entry.value().status == status)
            .count()
    }

    /// Get total number of workflows in the projection
    pub fn total_workflows(&self) -> usize {
        self.states.len()
    }

    /// Get the projection name
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Handle WorkflowEvent::Started
    fn handle_workflow_started(&self, workflow_id: WorkflowId, started_at: Timestamptz) {
        let state = WorkflowState {
            workflow_id,
            status: WorkflowStatus::Running,
            started_at,
            completed_at: None,
            completed_steps: Vec::new(),
            failed_steps: HashMap::new(),
            error: None,
            last_updated: started_at,
        };
        self.states.insert(workflow_id, state);
    }

    /// Handle WorkflowEvent::StepCompleted
    fn handle_step_completed(
        &self,
        workflow_id: WorkflowId,
        step_id: String,
        completed_at: Timestamptz,
    ) {
        if let Some(mut entry) = self.states.get_mut(&workflow_id) {
            let state = entry.value_mut();
            if !state.completed_steps.contains(&step_id) {
                state.completed_steps.push(step_id);
            }
            state.last_updated = completed_at;
        }
    }

    /// Handle WorkflowEvent::StepFailed
    fn handle_step_failed(
        &self,
        workflow_id: WorkflowId,
        step_id: String,
        error: String,
        failed_at: Timestamptz,
    ) {
        if let Some(mut entry) = self.states.get_mut(&workflow_id) {
            let state = entry.value_mut();
            state.failed_steps.insert(step_id, error);
            state.last_updated = failed_at;
        }
    }

    /// Handle WorkflowEvent::Completed
    fn handle_workflow_completed(&self, workflow_id: WorkflowId, completed_at: Timestamptz) {
        if let Some(mut entry) = self.states.get_mut(&workflow_id) {
            let state = entry.value_mut();
            state.status = WorkflowStatus::Completed;
            state.completed_at = Some(completed_at);
            state.last_updated = completed_at;
        }
    }

    /// Handle WorkflowEvent::Failed
    fn handle_workflow_failed(
        &self,
        workflow_id: WorkflowId,
        error: String,
        failed_at: Timestamptz,
    ) {
        if let Some(mut entry) = self.states.get_mut(&workflow_id) {
            let state = entry.value_mut();
            state.status = WorkflowStatus::Failed;
            state.error = Some(error);
            state.completed_at = Some(failed_at);
            state.last_updated = failed_at;
        }
    }

    /// Handle WorkflowEvent::Paused
    fn handle_workflow_paused(&self, workflow_id: WorkflowId, paused_at: Timestamptz) {
        if let Some(mut entry) = self.states.get_mut(&workflow_id) {
            let state = entry.value_mut();
            state.status = WorkflowStatus::Paused;
            state.last_updated = paused_at;
        }
    }

    /// Handle WorkflowEvent::Resumed
    fn handle_workflow_resumed(&self, workflow_id: WorkflowId, resumed_at: Timestamptz) {
        if let Some(mut entry) = self.states.get_mut(&workflow_id) {
            let state = entry.value_mut();
            state.status = WorkflowStatus::Running;
            state.last_updated = resumed_at;
        }
    }

    /// Handle WorkflowEvent::Cancelled
    fn handle_workflow_cancelled(&self, workflow_id: WorkflowId, cancelled_at: Timestamptz) {
        if let Some(mut entry) = self.states.get_mut(&workflow_id) {
            let state = entry.value_mut();
            state.status = WorkflowStatus::Cancelled;
            state.completed_at = Some(cancelled_at);
            state.last_updated = cancelled_at;
        }
    }

    /// Subscribe to workflow events from the event bus
    ///
    /// This starts a background task that listens for workflow events and updates the projection.
    /// Returns a join handle that can be used to cancel the subscription.
    ///
    /// # Arguments
    ///
    /// * `event_bus` - The event bus to subscribe to
    ///
    /// # Returns
    ///
    /// A join handle for the background task
    pub fn subscribe_to_events(self: Arc<Self>, event_bus: Arc<EventBus>) -> JoinHandle<()> {
        tokio::spawn(async move {
            let mut receiver = event_bus.subscribe_workflow_events();

            info!("WorkflowStateProjection subscribed to workflow events");

            loop {
                match receiver.recv().await {
                    Ok(event) => match &event {
                        WorkflowEvent::Started {
                            workflow_id,
                            started_at,
                        } => {
                            self.handle_workflow_started(*workflow_id, *started_at);
                        }
                        WorkflowEvent::StepCompleted {
                            workflow_id,
                            step_id,
                            completed_at,
                        } => {
                            self.handle_step_completed(
                                *workflow_id,
                                step_id.clone(),
                                *completed_at,
                            );
                        }
                        WorkflowEvent::StepFailed {
                            workflow_id,
                            step_id,
                            error,
                            failed_at,
                        } => {
                            self.handle_step_failed(
                                *workflow_id,
                                step_id.clone(),
                                error.clone(),
                                *failed_at,
                            );
                        }
                        WorkflowEvent::Completed {
                            workflow_id,
                            completed_at,
                        } => {
                            self.handle_workflow_completed(*workflow_id, *completed_at);
                        }
                        WorkflowEvent::Failed {
                            workflow_id,
                            error,
                            failed_at,
                        } => {
                            self.handle_workflow_failed(*workflow_id, error.clone(), *failed_at);
                        }
                        WorkflowEvent::Paused {
                            workflow_id,
                            paused_at,
                        } => {
                            self.handle_workflow_paused(*workflow_id, *paused_at);
                        }
                        WorkflowEvent::Resumed {
                            workflow_id,
                            resumed_at,
                        } => {
                            self.handle_workflow_resumed(*workflow_id, *resumed_at);
                        }
                        WorkflowEvent::Cancelled {
                            workflow_id,
                            reason: _,
                            cancelled_at,
                        } => {
                            self.handle_workflow_cancelled(*workflow_id, *cancelled_at);
                        }
                    },
                    Err(e) => {
                        error!(error = %e, "Error receiving workflow event in projection");
                        // On lag error, receiver is still valid, continue
                        if matches!(e, tokio::sync::broadcast::error::RecvError::Lagged(_)) {
                            continue;
                        }
                        // On closed error, exit the loop
                        break;
                    }
                }
            }

            info!("WorkflowStateProjection event subscription ended");
        })
    }

    /// Clear all workflows from the projection (for testing)
    #[cfg(test)]
    pub fn clear(&self) {
        self.states.clear();
    }
}

impl Default for WorkflowStateProjection {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl orcher_events::projections::Projection for WorkflowStateProjection {
    type State = HashMap<WorkflowId, WorkflowState>;

    fn name(&self) -> &str {
        &self.name
    }

    async fn project(&mut self, event: &StoredEvent) -> Result<(), Box<dyn Error>> {
        // Only process workflow events
        if !self.handles_event_type(&event.event_type) {
            return Ok(());
        }

        // Extract workflow ID from aggregate
        let _workflow_id = match event.stream_id.to_aggregate_id() {
            Some(AggregateId::Workflow(id)) => id,
            _ => return Ok(()), // Not a workflow event
        };

        // Parse the workflow event from payload
        let workflow_event: WorkflowEvent = serde_json::from_value(event.payload.clone())?;

        match workflow_event {
            WorkflowEvent::Started {
                workflow_id,
                started_at,
            } => {
                self.handle_workflow_started(workflow_id, started_at);
            }
            WorkflowEvent::StepCompleted {
                workflow_id,
                step_id,
                completed_at,
            } => {
                self.handle_step_completed(workflow_id, step_id, completed_at);
            }
            WorkflowEvent::StepFailed {
                workflow_id,
                step_id,
                error,
                failed_at,
            } => {
                self.handle_step_failed(workflow_id, step_id, error, failed_at);
            }
            WorkflowEvent::Completed {
                workflow_id,
                completed_at,
            } => {
                self.handle_workflow_completed(workflow_id, completed_at);
            }
            WorkflowEvent::Failed {
                workflow_id,
                error,
                failed_at,
            } => {
                self.handle_workflow_failed(workflow_id, error, failed_at);
            }
            WorkflowEvent::Paused {
                workflow_id,
                paused_at,
            } => {
                self.handle_workflow_paused(workflow_id, paused_at);
            }
            WorkflowEvent::Resumed {
                workflow_id,
                resumed_at,
            } => {
                self.handle_workflow_resumed(workflow_id, resumed_at);
            }
            WorkflowEvent::Cancelled {
                workflow_id,
                reason: _,
                cancelled_at,
            } => {
                self.handle_workflow_cancelled(workflow_id, cancelled_at);
            }
        }

        // Update last processed sequence
        *self.last_sequence.write().await = event.stream_position;

        Ok(())
    }

    async fn get_state(&self) -> Result<Self::State, Box<dyn Error>> {
        let mut state = HashMap::new();
        for entry in self.states.iter() {
            state.insert(*entry.key(), entry.value().clone());
        }
        Ok(state)
    }

    async fn reset(&mut self) -> Result<(), Box<dyn Error>> {
        self.states.clear();
        *self.last_sequence.write().await = 0;
        Ok(())
    }

    fn handles_event_type(&self, event_type: &str) -> bool {
        matches!(
            event_type,
            "WorkflowStarted"
                | "WorkflowStepCompleted"
                | "WorkflowStepFailed"
                | "WorkflowCompleted"
                | "WorkflowFailed"
                | "WorkflowPaused"
                | "WorkflowResumed"
                | "WorkflowCancelled"
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_workflow_projection_started() {
        let projection = WorkflowStateProjection::new();
        let workflow_id = WorkflowId::new_v4();
        let started_at = Timestamptz::now();

        projection.handle_workflow_started(workflow_id, started_at);

        let state = projection.get_workflow_state(&workflow_id).unwrap();
        assert_eq!(state.status, WorkflowStatus::Running);
        assert_eq!(state.started_at, started_at);
        assert!(state.completed_steps.is_empty());
    }

    #[tokio::test]
    async fn test_workflow_projection_step_completed() {
        let projection = WorkflowStateProjection::new();
        let workflow_id = WorkflowId::new_v4();
        let started_at = Timestamptz::now();

        projection.handle_workflow_started(workflow_id, started_at);

        let completed_at = Timestamptz::now();
        projection.handle_step_completed(workflow_id, "step1".to_string(), completed_at);

        let state = projection.get_workflow_state(&workflow_id).unwrap();
        assert_eq!(state.completed_steps.len(), 1);
        assert!(state.completed_steps.contains(&"step1".to_string()));
    }

    #[tokio::test]
    async fn test_workflow_projection_step_failed() {
        let projection = WorkflowStateProjection::new();
        let workflow_id = WorkflowId::new_v4();

        projection.handle_workflow_started(workflow_id, Timestamptz::now());

        projection.handle_step_failed(
            workflow_id,
            "step1".to_string(),
            "Task failed".to_string(),
            Timestamptz::now(),
        );

        let state = projection.get_workflow_state(&workflow_id).unwrap();
        assert_eq!(state.failed_steps.len(), 1);
        assert_eq!(
            state.failed_steps.get("step1"),
            Some(&"Task failed".to_string())
        );
    }

    #[tokio::test]
    async fn test_workflow_projection_completed() {
        let projection = WorkflowStateProjection::new();
        let workflow_id = WorkflowId::new_v4();

        projection.handle_workflow_started(workflow_id, Timestamptz::now());

        let completed_at = Timestamptz::now();
        projection.handle_workflow_completed(workflow_id, completed_at);

        let state = projection.get_workflow_state(&workflow_id).unwrap();
        assert_eq!(state.status, WorkflowStatus::Completed);
        assert_eq!(state.completed_at, Some(completed_at));
    }

    #[tokio::test]
    async fn test_workflow_projection_failed() {
        let projection = WorkflowStateProjection::new();
        let workflow_id = WorkflowId::new_v4();

        projection.handle_workflow_started(workflow_id, Timestamptz::now());

        projection.handle_workflow_failed(
            workflow_id,
            "Workflow execution failed".to_string(),
            Timestamptz::now(),
        );

        let state = projection.get_workflow_state(&workflow_id).unwrap();
        assert_eq!(state.status, WorkflowStatus::Failed);
        assert_eq!(state.error, Some("Workflow execution failed".to_string()));
    }

    #[tokio::test]
    async fn test_workflow_projection_paused_resumed() {
        let projection = WorkflowStateProjection::new();
        let workflow_id = WorkflowId::new_v4();

        projection.handle_workflow_started(workflow_id, Timestamptz::now());

        projection.handle_workflow_paused(workflow_id, Timestamptz::now());

        let state = projection.get_workflow_state(&workflow_id).unwrap();
        assert_eq!(state.status, WorkflowStatus::Paused);

        projection.handle_workflow_resumed(workflow_id, Timestamptz::now());

        let state = projection.get_workflow_state(&workflow_id).unwrap();
        assert_eq!(state.status, WorkflowStatus::Running);
    }

    #[tokio::test]
    async fn test_workflow_projection_cancelled() {
        let projection = WorkflowStateProjection::new();
        let workflow_id = WorkflowId::new_v4();

        projection.handle_workflow_started(workflow_id, Timestamptz::now());

        projection.handle_workflow_cancelled(workflow_id, Timestamptz::now());

        let state = projection.get_workflow_state(&workflow_id).unwrap();
        assert_eq!(state.status, WorkflowStatus::Cancelled);
    }

    #[tokio::test]
    async fn test_workflow_projection_count_by_status() {
        let projection = WorkflowStateProjection::new();

        for i in 0..5 {
            let workflow_id = WorkflowId::new_v4();
            projection.handle_workflow_started(workflow_id, Timestamptz::now());

            if i < 2 {
                projection.handle_workflow_completed(workflow_id, Timestamptz::now());
            }
        }

        assert_eq!(projection.count_by_status(WorkflowStatus::Running), 3);
        assert_eq!(projection.count_by_status(WorkflowStatus::Completed), 2);
        assert_eq!(projection.total_workflows(), 5);
    }

    #[tokio::test]
    async fn test_workflow_projection_reset() {
        let projection = WorkflowStateProjection::new();
        let workflow_id = WorkflowId::new_v4();

        projection.handle_workflow_started(workflow_id, Timestamptz::now());

        assert_eq!(projection.total_workflows(), 1);

        projection.clear();
        assert_eq!(projection.total_workflows(), 0);
    }
}
