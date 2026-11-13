//! Workflow execution instance events — tracks runtime state, variables, and outcomes.

use orcher_common::types::identifiers::WorkflowId;
use orcher_common::types::time::Timestamptz;
use serde::{Deserialize, Serialize};

/// State changes for a single workflow execution run.
///
/// - `WorkflowEvent`: Tracks the workflow definition and its step execution
/// - `ExecutionEvent`: Tracks a specific runtime instance of a workflow
/// - A single workflow can have multiple executions, each tracked separately.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ExecutionEvent {
    /// New execution instance created for a workflow.
    Created {
        execution_id: String,
        workflow_id: WorkflowId,
        trigger: String,
        created_at: Timestamptz,
    },

    /// Execution began processing.
    Started {
        execution_id: String,
        started_at: Timestamptz,
    },

    /// A workflow variable was set or updated.
    VariableSet {
        execution_id: String,
        key: String,
        value: serde_json::Value,
        set_at: Timestamptz,
    },

    /// Global execution context was replaced.
    ContextUpdated {
        execution_id: String,
        context: serde_json::Value,
        updated_at: Timestamptz,
    },

    /// All steps finished successfully.
    Completed {
        execution_id: String,
        completed_at: Timestamptz,
        #[serde(skip_serializing_if = "Option::is_none")]
        result: Option<serde_json::Value>,
    },

    /// Execution hit an unrecoverable error.
    Failed {
        execution_id: String,
        error: String,
        failed_at: Timestamptz,
    },

    /// Execution was cancelled by a user or the system.
    Cancelled {
        execution_id: String,
        reason: String,
        cancelled_at: Timestamptz,
    },

    /// Execution exceeded its maximum allowed duration.
    TimedOut {
        execution_id: String,
        timeout_seconds: u64,
        timed_out_at: Timestamptz,
    },
}

impl ExecutionEvent {
    /// The execution ID common to every variant.
    pub fn execution_id(&self) -> &str {
        match self {
            Self::Created { execution_id, .. } => execution_id,
            Self::Started { execution_id, .. } => execution_id,
            Self::VariableSet { execution_id, .. } => execution_id,
            Self::ContextUpdated { execution_id, .. } => execution_id,
            Self::Completed { execution_id, .. } => execution_id,
            Self::Failed { execution_id, .. } => execution_id,
            Self::Cancelled { execution_id, .. } => execution_id,
            Self::TimedOut { execution_id, .. } => execution_id,
        }
    }

    /// Workflow ID, only present on the `Created` variant.
    pub fn workflow_id(&self) -> Option<&WorkflowId> {
        match self {
            Self::Created { workflow_id, .. } => Some(workflow_id),
            _ => None,
        }
    }

    /// When this event occurred.
    pub fn timestamp(&self) -> &Timestamptz {
        match self {
            Self::Created { created_at, .. } => created_at,
            Self::Started { started_at, .. } => started_at,
            Self::VariableSet { set_at, .. } => set_at,
            Self::ContextUpdated { updated_at, .. } => updated_at,
            Self::Completed { completed_at, .. } => completed_at,
            Self::Failed { failed_at, .. } => failed_at,
            Self::Cancelled { cancelled_at, .. } => cancelled_at,
            Self::TimedOut { timed_out_at, .. } => timed_out_at,
        }
    }

    /// Dotted type name, e.g. "execution.started".
    pub fn event_type(&self) -> &str {
        match self {
            Self::Created { .. } => "execution.created",
            Self::Started { .. } => "execution.started",
            Self::VariableSet { .. } => "execution.variable_set",
            Self::ContextUpdated { .. } => "execution.context_updated",
            Self::Completed { .. } => "execution.completed",
            Self::Failed { .. } => "execution.failed",
            Self::Cancelled { .. } => "execution.cancelled",
            Self::TimedOut { .. } => "execution.timed_out",
        }
    }

    /// Returns `true` if the execution will not continue after this event.
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            Self::Completed { .. }
                | Self::Failed { .. }
                | Self::Cancelled { .. }
                | Self::TimedOut { .. }
        )
    }

    /// Returns `true` for `Failed` or `TimedOut`.
    pub fn is_failure(&self) -> bool {
        matches!(self, Self::Failed { .. } | Self::TimedOut { .. })
    }

    /// Returns `true` for `Completed`.
    pub fn is_success(&self) -> bool {
        matches!(self, Self::Completed { .. })
    }

    /// Returns `true` for `VariableSet` or `ContextUpdated`.
    pub fn is_state_change(&self) -> bool {
        matches!(self, Self::VariableSet { .. } | Self::ContextUpdated { .. })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use orcher_common::types::identifiers::Identifier;
    use serde_json::json;

    #[test]
    fn test_execution_event_created() {
        let execution_id = "exec_123".to_string();
        let workflow_id = WorkflowId::generate();
        let created_at = Timestamptz::now();

        let event = ExecutionEvent::Created {
            execution_id: execution_id.clone(),
            workflow_id,
            trigger: "manual".to_string(),
            created_at,
        };

        assert_eq!(event.execution_id(), &execution_id);
        assert_eq!(event.workflow_id(), Some(&workflow_id));
        assert_eq!(event.timestamp(), &created_at);
        assert_eq!(event.event_type(), "execution.created");
        assert!(!event.is_terminal());
        assert!(!event.is_failure());
        assert!(!event.is_success());
        assert!(!event.is_state_change());
    }

    #[test]
    fn test_execution_event_started() {
        let execution_id = "exec_123".to_string();
        let started_at = Timestamptz::now();

        let event = ExecutionEvent::Started {
            execution_id: execution_id.clone(),
            started_at,
        };

        assert_eq!(event.execution_id(), &execution_id);
        assert_eq!(event.workflow_id(), None);
        assert_eq!(event.timestamp(), &started_at);
        assert_eq!(event.event_type(), "execution.started");
        assert!(!event.is_terminal());
    }

    #[test]
    fn test_execution_event_variable_set() {
        let execution_id = "exec_123".to_string();
        let set_at = Timestamptz::now();
        let value = json!({"user_id": 42});

        let event = ExecutionEvent::VariableSet {
            execution_id: execution_id.clone(),
            key: "user_id".to_string(),
            value: value.clone(),
            set_at,
        };

        assert_eq!(event.execution_id(), &execution_id);
        assert_eq!(event.event_type(), "execution.variable_set");
        assert!(event.is_state_change());
        assert!(!event.is_terminal());
    }

    #[test]
    fn test_execution_event_context_updated() {
        let execution_id = "exec_123".to_string();
        let updated_at = Timestamptz::now();
        let context = json!({"env": "production"});

        let event = ExecutionEvent::ContextUpdated {
            execution_id: execution_id.clone(),
            context: context.clone(),
            updated_at,
        };

        assert_eq!(event.execution_id(), &execution_id);
        assert_eq!(event.event_type(), "execution.context_updated");
        assert!(event.is_state_change());
        assert!(!event.is_terminal());
    }

    #[test]
    fn test_execution_event_completed() {
        let execution_id = "exec_123".to_string();
        let completed_at = Timestamptz::now();
        let result = Some(json!({"status": "success"}));

        let event = ExecutionEvent::Completed {
            execution_id: execution_id.clone(),
            completed_at,
            result,
        };

        assert_eq!(event.execution_id(), &execution_id);
        assert_eq!(event.event_type(), "execution.completed");
        assert!(event.is_terminal());
        assert!(!event.is_failure());
        assert!(event.is_success());
    }

    #[test]
    fn test_execution_event_failed() {
        let execution_id = "exec_123".to_string();
        let failed_at = Timestamptz::now();

        let event = ExecutionEvent::Failed {
            execution_id: execution_id.clone(),
            error: "Execution error".to_string(),
            failed_at,
        };

        assert_eq!(event.execution_id(), &execution_id);
        assert_eq!(event.event_type(), "execution.failed");
        assert!(event.is_terminal());
        assert!(event.is_failure());
        assert!(!event.is_success());
    }

    #[test]
    fn test_execution_event_cancelled() {
        let execution_id = "exec_123".to_string();
        let cancelled_at = Timestamptz::now();

        let event = ExecutionEvent::Cancelled {
            execution_id: execution_id.clone(),
            reason: "User requested".to_string(),
            cancelled_at,
        };

        assert_eq!(event.execution_id(), &execution_id);
        assert_eq!(event.event_type(), "execution.cancelled");
        assert!(event.is_terminal());
        assert!(!event.is_failure());
    }

    #[test]
    fn test_execution_event_timed_out() {
        let execution_id = "exec_123".to_string();
        let timed_out_at = Timestamptz::now();

        let event = ExecutionEvent::TimedOut {
            execution_id: execution_id.clone(),
            timeout_seconds: 300,
            timed_out_at,
        };

        assert_eq!(event.execution_id(), &execution_id);
        assert_eq!(event.event_type(), "execution.timed_out");
        assert!(event.is_terminal());
        assert!(event.is_failure());
    }

    #[test]
    fn test_execution_event_serialization() {
        let event = ExecutionEvent::Started {
            execution_id: "exec_123".to_string(),
            started_at: Timestamptz::now(),
        };

        let json = serde_json::to_string(&event).unwrap();
        let deserialized: ExecutionEvent = serde_json::from_str(&json).unwrap();

        assert_eq!(event, deserialized);
    }

    #[test]
    fn test_execution_event_clone() {
        let event = ExecutionEvent::Started {
            execution_id: "exec_123".to_string(),
            started_at: Timestamptz::now(),
        };

        let cloned = event.clone();
        assert_eq!(event, cloned);
    }

    #[test]
    fn test_all_execution_event_types() {
        let execution_id = "exec_123".to_string();
        let workflow_id = WorkflowId::generate();
        let now = Timestamptz::now();

        let events = vec![
            ExecutionEvent::Created {
                execution_id: execution_id.clone(),
                workflow_id,
                trigger: "manual".to_string(),
                created_at: now,
            },
            ExecutionEvent::Started {
                execution_id: execution_id.clone(),
                started_at: now,
            },
            ExecutionEvent::VariableSet {
                execution_id: execution_id.clone(),
                key: "var".to_string(),
                value: json!(42),
                set_at: now,
            },
            ExecutionEvent::ContextUpdated {
                execution_id: execution_id.clone(),
                context: json!({}),
                updated_at: now,
            },
            ExecutionEvent::Completed {
                execution_id: execution_id.clone(),
                completed_at: now,
                result: None,
            },
            ExecutionEvent::Failed {
                execution_id: execution_id.clone(),
                error: "error".to_string(),
                failed_at: now,
            },
            ExecutionEvent::Cancelled {
                execution_id: execution_id.clone(),
                reason: "reason".to_string(),
                cancelled_at: now,
            },
            ExecutionEvent::TimedOut {
                execution_id: execution_id.clone(),
                timeout_seconds: 300,
                timed_out_at: now,
            },
        ];

        // All events should have the same execution_id
        for event in &events {
            assert_eq!(event.execution_id(), &execution_id);
        }

        // All events should have a valid event_type
        for event in &events {
            assert!(!event.event_type().is_empty());
        }
    }

    #[test]
    fn test_terminal_events() {
        let execution_id = "exec_123".to_string();
        let now = Timestamptz::now();

        let terminal_events = vec![
            ExecutionEvent::Completed {
                execution_id: execution_id.clone(),
                completed_at: now,
                result: None,
            },
            ExecutionEvent::Failed {
                execution_id: execution_id.clone(),
                error: "error".to_string(),
                failed_at: now,
            },
            ExecutionEvent::Cancelled {
                execution_id: execution_id.clone(),
                reason: "reason".to_string(),
                cancelled_at: now,
            },
            ExecutionEvent::TimedOut {
                execution_id: execution_id.clone(),
                timeout_seconds: 300,
                timed_out_at: now,
            },
        ];

        for event in terminal_events {
            assert!(event.is_terminal());
        }
    }

    #[test]
    fn test_state_change_events() {
        let execution_id = "exec_123".to_string();
        let now = Timestamptz::now();

        let state_events = vec![
            ExecutionEvent::VariableSet {
                execution_id: execution_id.clone(),
                key: "var".to_string(),
                value: json!(42),
                set_at: now,
            },
            ExecutionEvent::ContextUpdated {
                execution_id: execution_id.clone(),
                context: json!({}),
                updated_at: now,
            },
        ];

        for event in state_events {
            assert!(event.is_state_change());
        }
    }
}
