//! Workflow lifecycle events: started -> step progression -> completed/failed/cancelled.

use orcher_common::types::identifiers::WorkflowId;
use orcher_common::types::time::Timestamptz;
use serde::{Deserialize, Serialize};

/// Every state transition in a workflow's life.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum WorkflowEvent {
    /// Workflow execution has begun.
    Started {
        workflow_id: WorkflowId,
        started_at: Timestamptz,
    },

    /// An individual step finished successfully.
    StepCompleted {
        workflow_id: WorkflowId,
        step_id: String,
        completed_at: Timestamptz,
    },

    /// A step failed during execution.
    StepFailed {
        workflow_id: WorkflowId,
        step_id: String,
        error: String,
        failed_at: Timestamptz,
    },

    /// All steps done, workflow succeeded (terminal).
    Completed {
        workflow_id: WorkflowId,
        completed_at: Timestamptz,
    },

    /// Workflow failed due to a step failure or other error (terminal).
    Failed {
        workflow_id: WorkflowId,
        error: String,
        failed_at: Timestamptz,
    },

    /// Execution suspended; can be resumed later.
    Paused {
        workflow_id: WorkflowId,
        paused_at: Timestamptz,
    },

    /// Previously paused workflow continues from where it left off.
    Resumed {
        workflow_id: WorkflowId,
        resumed_at: Timestamptz,
    },

    /// Cancelled by user or system (terminal).
    Cancelled {
        workflow_id: WorkflowId,
        reason: String,
        cancelled_at: Timestamptz,
    },
}

impl WorkflowEvent {
    /// The workflow this event belongs to.
    pub fn workflow_id(&self) -> &WorkflowId {
        match self {
            Self::Started { workflow_id, .. } => workflow_id,
            Self::StepCompleted { workflow_id, .. } => workflow_id,
            Self::StepFailed { workflow_id, .. } => workflow_id,
            Self::Completed { workflow_id, .. } => workflow_id,
            Self::Failed { workflow_id, .. } => workflow_id,
            Self::Paused { workflow_id, .. } => workflow_id,
            Self::Resumed { workflow_id, .. } => workflow_id,
            Self::Cancelled { workflow_id, .. } => workflow_id,
        }
    }

    /// When this event happened.
    pub fn timestamp(&self) -> &Timestamptz {
        match self {
            Self::Started { started_at, .. } => started_at,
            Self::StepCompleted { completed_at, .. } => completed_at,
            Self::StepFailed { failed_at, .. } => failed_at,
            Self::Completed { completed_at, .. } => completed_at,
            Self::Failed { failed_at, .. } => failed_at,
            Self::Paused { paused_at, .. } => paused_at,
            Self::Resumed { resumed_at, .. } => resumed_at,
            Self::Cancelled { cancelled_at, .. } => cancelled_at,
        }
    }

    /// Dotted type name, e.g. "workflow.started".
    pub fn event_type(&self) -> &str {
        match self {
            Self::Started { .. } => "workflow.started",
            Self::StepCompleted { .. } => "workflow.step_completed",
            Self::StepFailed { .. } => "workflow.step_failed",
            Self::Completed { .. } => "workflow.completed",
            Self::Failed { .. } => "workflow.failed",
            Self::Paused { .. } => "workflow.paused",
            Self::Resumed { .. } => "workflow.resumed",
            Self::Cancelled { .. } => "workflow.cancelled",
        }
    }

    /// True for Completed, Failed, or Cancelled.
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            Self::Completed { .. } | Self::Failed { .. } | Self::Cancelled { .. }
        )
    }

    /// True for Failed or StepFailed.
    pub fn is_failure(&self) -> bool {
        matches!(self, Self::Failed { .. } | Self::StepFailed { .. })
    }

    /// True for Completed.
    pub fn is_success(&self) -> bool {
        matches!(self, Self::Completed { .. })
    }

    /// True for StepCompleted or StepFailed.
    pub fn is_step_event(&self) -> bool {
        matches!(self, Self::StepCompleted { .. } | Self::StepFailed { .. })
    }

    /// The step ID, if this is a step-level event.
    pub fn step_id(&self) -> Option<&String> {
        match self {
            Self::StepCompleted { step_id, .. } => Some(step_id),
            Self::StepFailed { step_id, .. } => Some(step_id),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use orcher_common::types::identifiers::Identifier;

    #[test]
    fn test_workflow_event_started() {
        let workflow_id = WorkflowId::generate();
        let started_at = Timestamptz::now();
        let event = WorkflowEvent::Started {
            workflow_id: workflow_id.clone(),
            started_at,
        };

        assert_eq!(event.workflow_id(), &workflow_id);
        assert_eq!(event.timestamp(), &started_at);
        assert_eq!(event.event_type(), "workflow.started");
        assert!(!event.is_terminal());
        assert!(!event.is_failure());
        assert!(!event.is_success());
        assert!(!event.is_step_event());
        assert_eq!(event.step_id(), None);
    }

    #[test]
    fn test_workflow_event_step_completed() {
        let workflow_id = WorkflowId::generate();
        let completed_at = Timestamptz::now();
        let step_id = "step_1".to_string();

        let event = WorkflowEvent::StepCompleted {
            workflow_id: workflow_id.clone(),
            step_id: step_id.clone(),
            completed_at,
        };

        assert_eq!(event.workflow_id(), &workflow_id);
        assert_eq!(event.timestamp(), &completed_at);
        assert_eq!(event.event_type(), "workflow.step_completed");
        assert!(!event.is_terminal());
        assert!(!event.is_failure());
        assert!(event.is_step_event());
        assert_eq!(event.step_id(), Some(&step_id));
    }

    #[test]
    fn test_workflow_event_step_failed() {
        let workflow_id = WorkflowId::generate();
        let failed_at = Timestamptz::now();

        let event = WorkflowEvent::StepFailed {
            workflow_id: workflow_id.clone(),
            step_id: "step_2".to_string(),
            error: "Step execution failed".to_string(),
            failed_at,
        };

        assert_eq!(event.workflow_id(), &workflow_id);
        assert_eq!(event.event_type(), "workflow.step_failed");
        assert!(!event.is_terminal());
        assert!(event.is_failure());
        assert!(event.is_step_event());
        assert_eq!(event.step_id(), Some(&"step_2".to_string()));
    }

    #[test]
    fn test_workflow_event_completed() {
        let workflow_id = WorkflowId::generate();
        let completed_at = Timestamptz::now();

        let event = WorkflowEvent::Completed {
            workflow_id: workflow_id.clone(),
            completed_at,
        };

        assert_eq!(event.workflow_id(), &workflow_id);
        assert_eq!(event.event_type(), "workflow.completed");
        assert!(event.is_terminal());
        assert!(!event.is_failure());
        assert!(event.is_success());
        assert!(!event.is_step_event());
    }

    #[test]
    fn test_workflow_event_failed() {
        let workflow_id = WorkflowId::generate();
        let failed_at = Timestamptz::now();

        let event = WorkflowEvent::Failed {
            workflow_id: workflow_id.clone(),
            error: "Workflow timeout".to_string(),
            failed_at,
        };

        assert_eq!(event.workflow_id(), &workflow_id);
        assert_eq!(event.event_type(), "workflow.failed");
        assert!(event.is_terminal());
        assert!(event.is_failure());
        assert!(!event.is_success());
    }

    #[test]
    fn test_workflow_event_paused() {
        let workflow_id = WorkflowId::generate();
        let paused_at = Timestamptz::now();

        let event = WorkflowEvent::Paused {
            workflow_id: workflow_id.clone(),
            paused_at,
        };

        assert_eq!(event.workflow_id(), &workflow_id);
        assert_eq!(event.event_type(), "workflow.paused");
        assert!(!event.is_terminal());
        assert!(!event.is_failure());
    }

    #[test]
    fn test_workflow_event_resumed() {
        let workflow_id = WorkflowId::generate();
        let resumed_at = Timestamptz::now();

        let event = WorkflowEvent::Resumed {
            workflow_id: workflow_id.clone(),
            resumed_at,
        };

        assert_eq!(event.workflow_id(), &workflow_id);
        assert_eq!(event.event_type(), "workflow.resumed");
        assert!(!event.is_terminal());
    }

    #[test]
    fn test_workflow_event_cancelled() {
        let workflow_id = WorkflowId::generate();
        let cancelled_at = Timestamptz::now();

        let event = WorkflowEvent::Cancelled {
            workflow_id: workflow_id.clone(),
            reason: "User requested".to_string(),
            cancelled_at,
        };

        assert_eq!(event.workflow_id(), &workflow_id);
        assert_eq!(event.event_type(), "workflow.cancelled");
        assert!(event.is_terminal());
        assert!(!event.is_failure());
        assert!(!event.is_success());
    }

    #[test]
    fn test_workflow_event_serialization() {
        let event = WorkflowEvent::Started {
            workflow_id: WorkflowId::generate(),
            started_at: Timestamptz::now(),
        };

        let json = serde_json::to_string(&event).unwrap();
        let deserialized: WorkflowEvent = serde_json::from_str(&json).unwrap();

        assert_eq!(event, deserialized);
    }

    #[test]
    fn test_workflow_event_clone() {
        let event = WorkflowEvent::Started {
            workflow_id: WorkflowId::generate(),
            started_at: Timestamptz::now(),
        };

        let cloned = event.clone();
        assert_eq!(event, cloned);
    }

    #[test]
    fn test_all_workflow_event_types() {
        let workflow_id = WorkflowId::generate();
        let now = Timestamptz::now();

        let events = vec![
            WorkflowEvent::Started {
                workflow_id: workflow_id.clone(),
                started_at: now,
            },
            WorkflowEvent::StepCompleted {
                workflow_id: workflow_id.clone(),
                step_id: "step_1".to_string(),
                completed_at: now,
            },
            WorkflowEvent::StepFailed {
                workflow_id: workflow_id.clone(),
                step_id: "step_2".to_string(),
                error: "error".to_string(),
                failed_at: now,
            },
            WorkflowEvent::Completed {
                workflow_id: workflow_id.clone(),
                completed_at: now,
            },
            WorkflowEvent::Failed {
                workflow_id: workflow_id.clone(),
                error: "error".to_string(),
                failed_at: now,
            },
            WorkflowEvent::Paused {
                workflow_id: workflow_id.clone(),
                paused_at: now,
            },
            WorkflowEvent::Resumed {
                workflow_id: workflow_id.clone(),
                resumed_at: now,
            },
            WorkflowEvent::Cancelled {
                workflow_id: workflow_id.clone(),
                reason: "reason".to_string(),
                cancelled_at: now,
            },
        ];

        // All events should have the same workflow_id
        for event in &events {
            assert_eq!(event.workflow_id(), &workflow_id);
        }

        // All events should have a valid event_type
        for event in &events {
            assert!(!event.event_type().is_empty());
        }
    }

    #[test]
    fn test_terminal_events() {
        let workflow_id = WorkflowId::generate();
        let now = Timestamptz::now();

        let terminal_events = vec![
            WorkflowEvent::Completed {
                workflow_id: workflow_id.clone(),
                completed_at: now,
            },
            WorkflowEvent::Failed {
                workflow_id: workflow_id.clone(),
                error: "error".to_string(),
                failed_at: now,
            },
            WorkflowEvent::Cancelled {
                workflow_id: workflow_id.clone(),
                reason: "reason".to_string(),
                cancelled_at: now,
            },
        ];

        for event in terminal_events {
            assert!(event.is_terminal());
        }
    }

    #[test]
    fn test_step_events() {
        let workflow_id = WorkflowId::generate();
        let now = Timestamptz::now();

        let step_events = vec![
            WorkflowEvent::StepCompleted {
                workflow_id: workflow_id.clone(),
                step_id: "step_1".to_string(),
                completed_at: now,
            },
            WorkflowEvent::StepFailed {
                workflow_id: workflow_id.clone(),
                step_id: "step_2".to_string(),
                error: "error".to_string(),
                failed_at: now,
            },
        ];

        for event in step_events {
            assert!(event.is_step_event());
            assert!(event.step_id().is_some());
        }
    }
}
