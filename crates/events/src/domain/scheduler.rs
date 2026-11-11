//! Schedule lifecycle events: creation, triggers, misses, enable/disable.

use orcher_common::types::identifiers::WorkflowId;
use orcher_common::types::time::Timestamptz;
use serde::{Deserialize, Serialize};

/// Cron schedule state transitions and trigger history.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SchedulerEvent {
    /// A new cron schedule was created.
    ScheduleCreated {
        schedule_id: String,
        cron: String,
        workflow_id: WorkflowId,
        created_at: Timestamptz,
    },

    /// Schedule configuration was modified.
    ScheduleUpdated {
        schedule_id: String,
        changes: serde_json::Value,
        updated_at: Timestamptz,
    },

    /// Schedule was permanently removed.
    ScheduleDeleted {
        schedule_id: String,
        deleted_at: Timestamptz,
    },

    /// Cron fired and spawned an execution.
    ScheduleTriggered {
        schedule_id: String,
        execution_id: String,
        timestamp: Timestamptz,
    },

    /// A trigger window was missed (downtime, overload, etc.).
    ScheduleMissed {
        schedule_id: String,
        missed_at: Timestamptz,
        reason: String,
    },

    /// Schedule was activated and will begin triggering.
    ScheduleEnabled {
        schedule_id: String,
        enabled_at: Timestamptz,
    },

    /// Schedule was paused; config is kept for re-enable.
    ScheduleDisabled {
        schedule_id: String,
        disabled_at: Timestamptz,
    },
}

impl SchedulerEvent {
    /// The schedule ID common to every variant.
    pub fn schedule_id(&self) -> &str {
        match self {
            Self::ScheduleCreated { schedule_id, .. } => schedule_id,
            Self::ScheduleUpdated { schedule_id, .. } => schedule_id,
            Self::ScheduleDeleted { schedule_id, .. } => schedule_id,
            Self::ScheduleTriggered { schedule_id, .. } => schedule_id,
            Self::ScheduleMissed { schedule_id, .. } => schedule_id,
            Self::ScheduleEnabled { schedule_id, .. } => schedule_id,
            Self::ScheduleDisabled { schedule_id, .. } => schedule_id,
        }
    }

    /// Workflow ID, only present on `ScheduleCreated`.
    pub fn workflow_id(&self) -> Option<&WorkflowId> {
        match self {
            Self::ScheduleCreated { workflow_id, .. } => Some(workflow_id),
            _ => None,
        }
    }

    /// Execution ID spawned by a trigger, if applicable.
    pub fn execution_id(&self) -> Option<&str> {
        match self {
            Self::ScheduleTriggered { execution_id, .. } => Some(execution_id),
            _ => None,
        }
    }

    /// When this event occurred.
    pub fn timestamp(&self) -> &Timestamptz {
        match self {
            Self::ScheduleCreated { created_at, .. } => created_at,
            Self::ScheduleUpdated { updated_at, .. } => updated_at,
            Self::ScheduleDeleted { deleted_at, .. } => deleted_at,
            Self::ScheduleTriggered { timestamp, .. } => timestamp,
            Self::ScheduleMissed { missed_at, .. } => missed_at,
            Self::ScheduleEnabled { enabled_at, .. } => enabled_at,
            Self::ScheduleDisabled { disabled_at, .. } => disabled_at,
        }
    }

    /// Dotted type name, e.g. "scheduler.schedule_triggered".
    pub fn event_type(&self) -> &str {
        match self {
            Self::ScheduleCreated { .. } => "scheduler.schedule_created",
            Self::ScheduleUpdated { .. } => "scheduler.schedule_updated",
            Self::ScheduleDeleted { .. } => "scheduler.schedule_deleted",
            Self::ScheduleTriggered { .. } => "scheduler.schedule_triggered",
            Self::ScheduleMissed { .. } => "scheduler.schedule_missed",
            Self::ScheduleEnabled { .. } => "scheduler.schedule_enabled",
            Self::ScheduleDisabled { .. } => "scheduler.schedule_disabled",
        }
    }

    /// Returns `true` if the schedule was deleted.
    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::ScheduleDeleted { .. })
    }

    /// Returns `true` for `ScheduleTriggered` or `ScheduleMissed`.
    pub fn is_trigger_event(&self) -> bool {
        matches!(
            self,
            Self::ScheduleTriggered { .. } | Self::ScheduleMissed { .. }
        )
    }

    /// Returns `true` for `ScheduleEnabled` or `ScheduleDisabled`.
    pub fn is_state_change(&self) -> bool {
        matches!(
            self,
            Self::ScheduleEnabled { .. } | Self::ScheduleDisabled { .. }
        )
    }

    /// `Some(true)` if enabled, `Some(false)` if disabled, `None` otherwise.
    pub fn is_active(&self) -> Option<bool> {
        match self {
            Self::ScheduleEnabled { .. } => Some(true),
            Self::ScheduleDisabled { .. } => Some(false),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use orcher_common::types::identifiers::Identifier;
    use serde_json::json;

    #[test]
    fn test_scheduler_event_created() {
        let schedule_id = "sched_123".to_string();
        let workflow_id = WorkflowId::generate();
        let created_at = Timestamptz::now();

        let event = SchedulerEvent::ScheduleCreated {
            schedule_id: schedule_id.clone(),
            cron: "0 0 * * *".to_string(),
            workflow_id: workflow_id.clone(),
            created_at,
        };

        assert_eq!(event.schedule_id(), &schedule_id);
        assert_eq!(event.workflow_id(), Some(&workflow_id));
        assert_eq!(event.timestamp(), &created_at);
        assert_eq!(event.event_type(), "scheduler.schedule_created");
        assert!(!event.is_terminal());
        assert!(!event.is_trigger_event());
        assert!(!event.is_state_change());
        assert_eq!(event.is_active(), None);
    }

    #[test]
    fn test_scheduler_event_updated() {
        let schedule_id = "sched_123".to_string();
        let updated_at = Timestamptz::now();
        let changes = json!({"cron": "0 1 * * *"});

        let event = SchedulerEvent::ScheduleUpdated {
            schedule_id: schedule_id.clone(),
            changes,
            updated_at,
        };

        assert_eq!(event.schedule_id(), &schedule_id);
        assert_eq!(event.event_type(), "scheduler.schedule_updated");
        assert!(!event.is_terminal());
    }

    #[test]
    fn test_scheduler_event_deleted() {
        let schedule_id = "sched_123".to_string();
        let deleted_at = Timestamptz::now();

        let event = SchedulerEvent::ScheduleDeleted {
            schedule_id: schedule_id.clone(),
            deleted_at,
        };

        assert_eq!(event.schedule_id(), &schedule_id);
        assert_eq!(event.event_type(), "scheduler.schedule_deleted");
        assert!(event.is_terminal());
    }

    #[test]
    fn test_scheduler_event_triggered() {
        let schedule_id = "sched_123".to_string();
        let execution_id = "exec_456".to_string();
        let timestamp = Timestamptz::now();

        let event = SchedulerEvent::ScheduleTriggered {
            schedule_id: schedule_id.clone(),
            execution_id: execution_id.clone(),
            timestamp,
        };

        assert_eq!(event.schedule_id(), &schedule_id);
        assert_eq!(event.execution_id(), Some(execution_id.as_str()));
        assert_eq!(event.event_type(), "scheduler.schedule_triggered");
        assert!(event.is_trigger_event());
        assert!(!event.is_terminal());
    }

    #[test]
    fn test_scheduler_event_missed() {
        let schedule_id = "sched_123".to_string();
        let missed_at = Timestamptz::now();

        let event = SchedulerEvent::ScheduleMissed {
            schedule_id: schedule_id.clone(),
            missed_at,
            reason: "System overloaded".to_string(),
        };

        assert_eq!(event.schedule_id(), &schedule_id);
        assert_eq!(event.event_type(), "scheduler.schedule_missed");
        assert!(event.is_trigger_event());
        assert!(!event.is_terminal());
    }

    #[test]
    fn test_scheduler_event_enabled() {
        let schedule_id = "sched_123".to_string();
        let enabled_at = Timestamptz::now();

        let event = SchedulerEvent::ScheduleEnabled {
            schedule_id: schedule_id.clone(),
            enabled_at,
        };

        assert_eq!(event.schedule_id(), &schedule_id);
        assert_eq!(event.event_type(), "scheduler.schedule_enabled");
        assert!(event.is_state_change());
        assert_eq!(event.is_active(), Some(true));
        assert!(!event.is_terminal());
    }

    #[test]
    fn test_scheduler_event_disabled() {
        let schedule_id = "sched_123".to_string();
        let disabled_at = Timestamptz::now();

        let event = SchedulerEvent::ScheduleDisabled {
            schedule_id: schedule_id.clone(),
            disabled_at,
        };

        assert_eq!(event.schedule_id(), &schedule_id);
        assert_eq!(event.event_type(), "scheduler.schedule_disabled");
        assert!(event.is_state_change());
        assert_eq!(event.is_active(), Some(false));
    }

    #[test]
    fn test_scheduler_event_serialization() {
        let event = SchedulerEvent::ScheduleEnabled {
            schedule_id: "sched_123".to_string(),
            enabled_at: Timestamptz::now(),
        };

        let json = serde_json::to_string(&event).unwrap();
        let deserialized: SchedulerEvent = serde_json::from_str(&json).unwrap();

        assert_eq!(event, deserialized);
    }

    #[test]
    fn test_scheduler_event_clone() {
        let event = SchedulerEvent::ScheduleEnabled {
            schedule_id: "sched_123".to_string(),
            enabled_at: Timestamptz::now(),
        };

        let cloned = event.clone();
        assert_eq!(event, cloned);
    }

    #[test]
    fn test_all_scheduler_event_types() {
        let schedule_id = "sched_123".to_string();
        let workflow_id = WorkflowId::generate();
        let now = Timestamptz::now();

        let events = vec![
            SchedulerEvent::ScheduleCreated {
                schedule_id: schedule_id.clone(),
                cron: "0 0 * * *".to_string(),
                workflow_id: workflow_id.clone(),
                created_at: now,
            },
            SchedulerEvent::ScheduleUpdated {
                schedule_id: schedule_id.clone(),
                changes: json!({}),
                updated_at: now,
            },
            SchedulerEvent::ScheduleDeleted {
                schedule_id: schedule_id.clone(),
                deleted_at: now,
            },
            SchedulerEvent::ScheduleTriggered {
                schedule_id: schedule_id.clone(),
                execution_id: "exec_456".to_string(),
                timestamp: now,
            },
            SchedulerEvent::ScheduleMissed {
                schedule_id: schedule_id.clone(),
                missed_at: now,
                reason: "reason".to_string(),
            },
            SchedulerEvent::ScheduleEnabled {
                schedule_id: schedule_id.clone(),
                enabled_at: now,
            },
            SchedulerEvent::ScheduleDisabled {
                schedule_id: schedule_id.clone(),
                disabled_at: now,
            },
        ];

        // All events should have the same schedule_id
        for event in &events {
            assert_eq!(event.schedule_id(), &schedule_id);
        }

        // All events should have a valid event_type
        for event in &events {
            assert!(!event.event_type().is_empty());
        }
    }

    #[test]
    fn test_trigger_events() {
        let schedule_id = "sched_123".to_string();
        let now = Timestamptz::now();

        let trigger_events = vec![
            SchedulerEvent::ScheduleTriggered {
                schedule_id: schedule_id.clone(),
                execution_id: "exec_456".to_string(),
                timestamp: now,
            },
            SchedulerEvent::ScheduleMissed {
                schedule_id: schedule_id.clone(),
                missed_at: now,
                reason: "reason".to_string(),
            },
        ];

        for event in trigger_events {
            assert!(event.is_trigger_event());
        }
    }

    #[test]
    fn test_state_change_events() {
        let schedule_id = "sched_123".to_string();
        let now = Timestamptz::now();

        let state_events = vec![
            SchedulerEvent::ScheduleEnabled {
                schedule_id: schedule_id.clone(),
                enabled_at: now,
            },
            SchedulerEvent::ScheduleDisabled {
                schedule_id: schedule_id.clone(),
                disabled_at: now,
            },
        ];

        for event in state_events {
            assert!(event.is_state_change());
        }
    }
}
