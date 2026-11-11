//! Journal entry to proto type mapping.

use orcher_common::OrcherResult;
use orcher_proto::orcher::v1::{
    journal_entry::Attributes, ChildWorkflowExecutionCompletedEventAttributes,
    ChildWorkflowExecutionFailedEventAttributes, ChildWorkflowExecutionStartedEventAttributes,
    ChildWorkflowExecutionTimedOutEventAttributes, EntryType, EventReceivedEventAttributes,
    Failure, JournalEntry as ProtoJournalEntry, TaskCompletedEventAttributes,
    TaskFailedEventAttributes, TaskScheduledEventAttributes, TaskStartedEventAttributes,
    TimerCanceledEventAttributes, TimerFiredEventAttributes, TimerStartedEventAttributes,
    UpdateAcceptedEventAttributes, UpdateCompletedEventAttributes,
    WorkflowExecutionCancelRequestedEventAttributes, WorkflowExecutionCanceledEventAttributes,
    WorkflowExecutionCompletedEventAttributes, WorkflowExecutionFailedEventAttributes,
    WorkflowExecutionStartedEventAttributes, WorkflowExecutionTimedOutEventAttributes,
};
use orcher_proto::prost_types::{Duration, Timestamp};
use orcher_storage::schema::types::journal::JournalEntry as StorageJournalEntry;
use serde_json::Value;
use tracing::{debug, warn};

/// Journal entry mapper for storage → proto conversion
pub struct JournalMapper;

impl JournalMapper {
    /// Create a new journal mapper
    pub fn new() -> Self {
        Self
    }

    /// Convert a storage journal entry to proto format
    ///
    /// # Arguments
    ///
    /// * `entry` - Storage journal entry to convert
    ///
    /// # Returns
    ///
    /// Proto journal entry ready for gRPC transmission
    pub fn to_proto(&self, entry: &StorageJournalEntry) -> OrcherResult<ProtoJournalEntry> {
        // Convert timestamp
        let timestamp = Some(Timestamp {
            seconds: entry.timestamp.timestamp(),
            nanos: entry.timestamp.timestamp_subsec_nanos() as i32,
        });

        // Map entry type string to proto enum
        let entry_type = self.map_entry_type(&entry.event_type)?;

        // Parse and map attributes based on entry type. A child's start is
        // journaled when the parent starts it, as ChildWorkflowExecutionInitiated;
        // workers see it as the child's start, its own entry the initiating one.
        let attributes = if entry.event_type == "ChildWorkflowExecutionInitiated" {
            self.map_child_workflow_initiated(&entry.payload, entry.event_id)
        } else {
            self.map_payload(&entry.event_type, &entry.payload)?
        };

        Ok(ProtoJournalEntry {
            entry_id: entry.event_id,
            timestamp,
            entry_type: entry_type as i32,
            version: entry.version as i64,
            task_id: entry.task_id,
            attributes,
        })
    }

    /// Convert multiple storage journal entries to proto format
    ///
    /// # Arguments
    ///
    /// * `entries` - Vector of storage journal entries
    ///
    /// # Returns
    ///
    /// Vector of proto journal entries
    pub fn to_proto_vec(
        &self,
        entries: &[StorageJournalEntry],
    ) -> OrcherResult<Vec<ProtoJournalEntry>> {
        entries.iter().map(|entry| self.to_proto(entry)).collect()
    }

    /// Map storage entry type string to proto EntryType enum
    fn map_entry_type(&self, event_type: &str) -> OrcherResult<EntryType> {
        let entry_type = match event_type {
            // Workflow execution entries
            "WorkflowExecutionStarted" => EntryType::WorkflowExecutionStarted,
            "WorkflowExecutionCompleted" => EntryType::WorkflowExecutionCompleted,
            "WorkflowExecutionFailed" => EntryType::WorkflowExecutionFailed,
            "WorkflowExecutionTimedOut" => EntryType::WorkflowExecutionTimedOut,
            "WorkflowExecutionCanceled" => EntryType::WorkflowExecutionCanceled,
            "WorkflowExecutionRestartedFresh" => EntryType::WorkflowExecutionRestartedFresh,
            "WorkflowExecutionCancelRequested" => EntryType::WorkflowExecutionCancelRequested,
            "WorkflowExecutionTerminated" => EntryType::WorkflowExecutionTerminated,

            // Task entries
            "TaskScheduled" => EntryType::TaskScheduled,
            "TaskStarted" => EntryType::TaskStarted,
            "TaskCompleted" => EntryType::TaskCompleted,
            "TaskFailed" => EntryType::TaskFailed,
            "TaskTimedOut" => EntryType::TaskTimedOut,
            "TaskCancelRequested" => EntryType::TaskCancelRequested,
            "TaskCanceled" => EntryType::TaskCanceled,

            // Timer entries
            "TimerStarted" => EntryType::TimerStarted,
            "TimerFired" => EntryType::TimerFired,
            "TimerCanceled" => EntryType::TimerCanceled,

            // Event entries
            "EventReceived" => EntryType::EventReceived,

            // A settled step: a task's result or failure, an inline step's
            // result.
            super::STEP_COMPLETED => EntryType::StepCompleted,

            // Child workflow entries
            "ChildWorkflowExecutionInitiated" | "ChildWorkflowExecutionStarted" => {
                EntryType::ChildWorkflowExecutionStarted
            }
            "ChildWorkflowExecutionCompleted" => EntryType::ChildWorkflowExecutionCompleted,
            "ChildWorkflowExecutionFailed" => EntryType::ChildWorkflowExecutionFailed,
            "ChildWorkflowExecutionTimedOut" => EntryType::ChildWorkflowExecutionTimedOut,
            "ChildWorkflowExecutionCanceled" => EntryType::ChildWorkflowExecutionCanceled,
            "ChildWorkflowExecutionTerminated" => EntryType::ChildWorkflowExecutionTerminated,

            // Update entries
            "UpdateAccepted" => EntryType::UpdateAccepted,
            "UpdateCompleted" => EntryType::UpdateCompleted,

            // Unknown entry type
            unknown => {
                warn!(entry_type = %unknown, "Unknown journal entry type, using Unspecified");
                EntryType::Unspecified
            }
        };

        Ok(entry_type)
    }

    /// A settled step's entry. One written before steps were recorded this
    /// way has another shape, and is sent without attributes, as it always
    /// was: its run's step results are rebuilt from the execution log.
    fn map_step_completed(payload: &Value) -> Option<Attributes> {
        match serde_json::from_value::<super::SettledStep>(payload.clone()) {
            Ok(step) => Some(Attributes::StepCompleted(step.attributes())),
            Err(e) => {
                debug!(error = %e, "StepCompleted entry of an earlier shape; no attributes");
                None
            }
        }
    }

    /// Map JSON payload to proto oneof attributes based on entry type
    fn map_payload(&self, event_type: &str, payload: &Value) -> OrcherResult<Option<Attributes>> {
        let result = match event_type {
            "WorkflowExecutionStarted" => self.map_workflow_started(payload)?,
            "WorkflowExecutionCompleted" => self.map_workflow_completed(payload)?,
            "WorkflowExecutionFailed" => self.map_workflow_failed(payload)?,
            "WorkflowExecutionTimedOut" => self.map_workflow_timed_out(payload)?,
            "WorkflowExecutionCanceled" => self.map_workflow_canceled(payload)?,
            "WorkflowExecutionCancelRequested" => self.map_workflow_cancel_requested(payload)?,

            "TaskScheduled" => self.map_task_scheduled(payload)?,
            "TaskStarted" => self.map_task_started(payload)?,
            "TaskCompleted" => self.map_task_completed(payload)?,
            "TaskFailed" => self.map_task_failed(payload)?,

            "TimerStarted" => self.map_timer_started(payload)?,
            "TimerFired" => self.map_timer_fired(payload)?,
            "TimerCanceled" => self.map_timer_canceled(payload)?,

            "ChildWorkflowExecutionStarted" => self.map_child_workflow_started(payload)?,
            "ChildWorkflowExecutionCompleted" => self.map_child_workflow_completed(payload)?,
            "ChildWorkflowExecutionFailed" => self.map_child_workflow_failed(payload)?,
            "ChildWorkflowExecutionTimedOut" => self.map_child_workflow_timed_out(payload)?,

            "UpdateAccepted" => self.map_update_accepted(payload)?,
            "UpdateCompleted" => self.map_update_completed(payload)?,

            "EventReceived" => self.map_event_received(payload)?,

            super::STEP_COMPLETED => Self::map_step_completed(payload),

            _ => {
                debug!(entry_type = %event_type, "No attribute mapping for entry type");
                None
            }
        };

        Ok(result)
    }

    // ========================================================================
    // WORKFLOW EXECUTION ATTRIBUTE MAPPERS
    // ========================================================================

    fn map_workflow_started(&self, attrs: &Value) -> OrcherResult<Option<Attributes>> {
        let workflow_type = attrs["workflow_type"]
            .as_str()
            .unwrap_or("unknown")
            .to_string();
        let task_queue = attrs["task_queue"]
            .as_str()
            .unwrap_or("default")
            .to_string();
        let input = attrs["input"].to_string().into_bytes();

        Ok(Some(Attributes::WorkflowExecutionStarted(
            WorkflowExecutionStartedEventAttributes {
                workflow_type,
                task_queue,
                input,
                execution_timeout: None,
                run_timeout: None,
                task_timeout: None,
                retry_policy: None,
                cron_schedule: String::new(),
                annotations: Default::default(),
                labels: Default::default(),
                started_by: String::new(),
            },
        )))
    }

    fn map_workflow_completed(&self, attrs: &Value) -> OrcherResult<Option<Attributes>> {
        let result = attrs["result"].to_string().into_bytes();

        Ok(Some(Attributes::WorkflowExecutionCompleted(
            WorkflowExecutionCompletedEventAttributes {
                result,
                execution_step_completed_event_id: 0,
            },
        )))
    }

    fn map_workflow_failed(&self, attrs: &Value) -> OrcherResult<Option<Attributes>> {
        let failure = if let Some(failure_obj) = attrs.get("failure") {
            // Handle nested failure object
            Some(Failure {
                non_retryable: false,
                message: failure_obj["message"]
                    .as_str()
                    .unwrap_or("Unknown failure")
                    .to_string(),
                source: failure_obj["source"].as_str().unwrap_or("").to_string(),
                stack_trace: failure_obj["stack_trace"]
                    .as_str()
                    .unwrap_or("")
                    .to_string(),
                cause: None,
                failure_type: failure_obj["failure_type"]
                    .as_str()
                    .unwrap_or("")
                    .to_string(),
                details: Vec::new(),
            })
        } else {
            // Handle legacy flat format
            let failure_reason = attrs["failure_reason"]
                .as_str()
                .unwrap_or("Unknown failure")
                .to_string();
            let failure_details = attrs["failure_details"].to_string();

            Some(Failure {
                non_retryable: false,
                message: failure_reason,
                source: String::new(),
                stack_trace: failure_details,
                cause: None,
                failure_type: String::new(),
                details: Vec::new(),
            })
        };

        Ok(Some(Attributes::WorkflowExecutionFailed(
            WorkflowExecutionFailedEventAttributes {
                failure,
                retry_policy: None,
                execution_step_completed_event_id: 0,
            },
        )))
    }

    fn map_workflow_timed_out(&self, _attrs: &Value) -> OrcherResult<Option<Attributes>> {
        Ok(Some(Attributes::WorkflowExecutionTimedOut(
            WorkflowExecutionTimedOutEventAttributes {
                retry_policy: None,
                execution_step_timed_out_event_id: 0,
            },
        )))
    }

    fn map_workflow_cancel_requested(&self, attrs: &Value) -> OrcherResult<Option<Attributes>> {
        Ok(Some(Attributes::WorkflowExecutionCancelRequested(
            WorkflowExecutionCancelRequestedEventAttributes {
                cause: attrs["reason"].as_str().unwrap_or_default().to_string(),
                external_workflow_execution_id: String::new(),
                external_execution_id: String::new(),
            },
        )))
    }

    fn map_workflow_canceled(&self, attrs: &Value) -> OrcherResult<Option<Attributes>> {
        let reason = attrs["reason"]
            .as_str()
            .unwrap_or("Canceled")
            .to_string()
            .into_bytes();

        Ok(Some(Attributes::WorkflowExecutionCanceled(
            WorkflowExecutionCanceledEventAttributes {
                execution_step_completed_event_id: 0,
                details: reason,
            },
        )))
    }

    // ========================================================================
    // TASK ATTRIBUTE MAPPERS
    // ========================================================================

    fn map_task_scheduled(&self, attrs: &Value) -> OrcherResult<Option<Attributes>> {
        let task_id = attrs["task_id"].as_str().unwrap_or("unknown").to_string();
        let task_type = attrs["task_type"].as_str().unwrap_or("unknown").to_string();
        let task_queue = attrs["task_queue"]
            .as_str()
            .unwrap_or("default")
            .to_string();
        let input = attrs["input"].to_string().into_bytes();

        Ok(Some(Attributes::TaskScheduled(
            TaskScheduledEventAttributes {
                task_id,
                task_type,
                task_queue,
                input,
                schedule_to_start_timeout: None,
                start_to_close_timeout: None,
                heartbeat_timeout: None,
                retry_policy: None,
                execution_step_completed_event_id: 0,
                headers: Default::default(),
            },
        )))
    }

    fn map_task_started(&self, attrs: &Value) -> OrcherResult<Option<Attributes>> {
        let scheduled_event_id = attrs["scheduled_event_id"].as_i64().unwrap_or(0);
        let identity = attrs["identity"].as_str().unwrap_or("unknown").to_string();

        Ok(Some(Attributes::TaskStarted(TaskStartedEventAttributes {
            scheduled_event_id,
            identity,
            request_id: String::new(),
            attempt: 0,
        })))
    }

    fn map_task_completed(&self, attrs: &Value) -> OrcherResult<Option<Attributes>> {
        let scheduled_event_id = attrs["scheduled_event_id"].as_i64().unwrap_or(0);
        let started_event_id = attrs["started_event_id"].as_i64().unwrap_or(0);

        // Decode base64 result if it's stored as string, otherwise handle byte array
        let result = if let Some(result_str) = attrs["result"].as_str() {
            // Base64 encoded string (new format)
            base64::Engine::decode(&base64::engine::general_purpose::STANDARD, result_str)
                .unwrap_or_else(|_| result_str.as_bytes().to_vec())
        } else if attrs["result"].is_array() {
            // Legacy byte array format - convert to actual bytes
            attrs["result"]
                .as_array()
                .map(|arr| {
                    arr.iter()
                        .filter_map(|v| v.as_u64().map(|n| n as u8))
                        .collect()
                })
                .unwrap_or_default()
        } else if attrs["result"].is_null() {
            // None: the result is the task's StepCompleted entry's.
            Vec::new()
        } else {
            // Fallback: serialize as JSON string
            attrs["result"].to_string().into_bytes()
        };

        Ok(Some(Attributes::TaskCompleted(
            TaskCompletedEventAttributes {
                scheduled_event_id,
                started_event_id,
                result,
                identity: String::new(),
            },
        )))
    }

    fn map_task_failed(&self, attrs: &Value) -> OrcherResult<Option<Attributes>> {
        let scheduled_event_id = attrs["scheduled_event_id"].as_i64().unwrap_or(0);
        let started_event_id = attrs["started_event_id"].as_i64().unwrap_or(0);

        let failure = if let Some(failure_obj) = attrs.get("failure") {
            // Handle nested failure object
            Some(Failure {
                non_retryable: false,
                message: failure_obj["message"]
                    .as_str()
                    .unwrap_or("Unknown failure")
                    .to_string(),
                source: failure_obj["source"].as_str().unwrap_or("").to_string(),
                stack_trace: failure_obj["stack_trace"]
                    .as_str()
                    .unwrap_or("")
                    .to_string(),
                cause: None,
                failure_type: failure_obj["failure_type"]
                    .as_str()
                    .unwrap_or("")
                    .to_string(),
                details: Vec::new(),
            })
        } else {
            // Handle legacy flat format
            let failure_reason = attrs["failure_reason"]
                .as_str()
                .unwrap_or("Unknown failure")
                .to_string();

            Some(Failure {
                non_retryable: false,
                message: failure_reason,
                source: String::new(),
                stack_trace: String::new(),
                cause: None,
                failure_type: String::new(),
                details: Vec::new(),
            })
        };

        Ok(Some(Attributes::TaskFailed(TaskFailedEventAttributes {
            scheduled_event_id,
            started_event_id,
            failure,
            identity: String::new(),
            retry_policy: None,
        })))
    }

    // ========================================================================
    // TIMER ATTRIBUTE MAPPERS
    // ========================================================================

    fn map_timer_started(&self, attrs: &Value) -> OrcherResult<Option<Attributes>> {
        let timer_id = attrs["timer_id"].as_str().unwrap_or("unknown").to_string();
        let duration_ms = attrs["duration_ms"].as_i64().unwrap_or(0);

        let start_to_fire_timeout = Some(Duration {
            seconds: duration_ms / 1000,
            nanos: ((duration_ms % 1000) * 1_000_000) as i32,
        });

        Ok(Some(Attributes::TimerStarted(
            TimerStartedEventAttributes {
                timer_id,
                start_to_fire_timeout,
                execution_step_completed_event_id: 0,
            },
        )))
    }

    fn map_timer_fired(&self, attrs: &Value) -> OrcherResult<Option<Attributes>> {
        let started_event_id = attrs["started_event_id"].as_i64().unwrap_or(0);
        let timer_id = attrs["timer_id"].as_str().unwrap_or("unknown").to_string();

        Ok(Some(Attributes::TimerFired(TimerFiredEventAttributes {
            started_event_id,
            timer_id,
        })))
    }

    fn map_timer_canceled(&self, attrs: &Value) -> OrcherResult<Option<Attributes>> {
        let started_event_id = attrs["started_event_id"].as_i64().unwrap_or(0);
        let timer_id = attrs["timer_id"].as_str().unwrap_or("unknown").to_string();

        Ok(Some(Attributes::TimerCanceled(
            TimerCanceledEventAttributes {
                started_event_id,
                timer_id,
                identity: String::new(),
                execution_step_completed_event_id: 0,
            },
        )))
    }

    // ========================================================================
    // CHILD WORKFLOW ATTRIBUTE MAPPERS
    // ========================================================================

    fn map_child_workflow_started(&self, attrs: &Value) -> OrcherResult<Option<Attributes>> {
        let initiated_event_id = attrs["initiated_event_id"].as_i64().unwrap_or(0);
        let workflow_id = attrs["child_workflow_id"]
            .as_str()
            .unwrap_or("unknown")
            .to_string();
        let execution_id = attrs["child_execution_id"]
            .as_str()
            .unwrap_or("unknown")
            .to_string();
        let workflow_type = attrs["child_workflow_type"]
            .as_str()
            .unwrap_or("unknown")
            .to_string();

        Ok(Some(Attributes::ChildWorkflowExecutionStarted(
            ChildWorkflowExecutionStartedEventAttributes {
                initiated_event_id,
                workflow_id,
                execution_id,
                workflow_type,
                namespace: String::new(),
                started_time: None,
            },
        )))
    }

    /// A child's start, as the parent journaled it when it started the child:
    /// the child's workflow id and type, and its run id when the entry has it
    /// (entries journaled before it did have none).
    fn map_child_workflow_initiated(&self, attrs: &Value, event_id: i64) -> Option<Attributes> {
        let text = |field: &str| attrs[field].as_str().unwrap_or_default().to_string();
        Some(Attributes::ChildWorkflowExecutionStarted(
            ChildWorkflowExecutionStartedEventAttributes {
                initiated_event_id: event_id,
                workflow_id: text("child_workflow_id"),
                execution_id: text("child_execution_id"),
                workflow_type: text("child_workflow_type"),
                namespace: String::new(),
                started_time: None,
            },
        ))
    }

    fn map_child_workflow_completed(&self, attrs: &Value) -> OrcherResult<Option<Attributes>> {
        let initiated_event_id = attrs["initiated_event_id"].as_i64().unwrap_or(0);
        let started_event_id = attrs["started_event_id"].as_i64().unwrap_or(0);
        let workflow_id = attrs["child_workflow_id"]
            .as_str()
            .unwrap_or("unknown")
            .to_string();
        let execution_id = attrs["child_execution_id"]
            .as_str()
            .unwrap_or("unknown")
            .to_string();
        let result = attrs["result"].to_string().into_bytes();

        Ok(Some(Attributes::ChildWorkflowExecutionCompleted(
            ChildWorkflowExecutionCompletedEventAttributes {
                initiated_event_id,
                started_event_id,
                workflow_id,
                execution_id,
                result,
                completed_time: None,
            },
        )))
    }

    fn map_child_workflow_failed(&self, attrs: &Value) -> OrcherResult<Option<Attributes>> {
        let initiated_event_id = attrs["initiated_event_id"].as_i64().unwrap_or(0);
        let started_event_id = attrs["started_event_id"].as_i64().unwrap_or(0);
        let workflow_id = attrs["child_workflow_id"]
            .as_str()
            .unwrap_or("unknown")
            .to_string();
        let execution_id = attrs["child_execution_id"]
            .as_str()
            .unwrap_or("unknown")
            .to_string();
        let failure_reason = attrs["failure_reason"]
            .as_str()
            .unwrap_or("Unknown failure")
            .to_string();
        // A child that was cancelled, timed out or terminated reaches its
        // parent as a failure — the only ending besides completion every SDK
        // reacts to — and this is what says which ending it was.
        let failure_type = attrs["failure_type"].as_str().unwrap_or("").to_string();

        let failure = Some(Failure {
            non_retryable: false,
            message: failure_reason,
            source: String::new(),
            stack_trace: String::new(),
            cause: None,
            failure_type,
            details: Vec::new(),
        });

        Ok(Some(Attributes::ChildWorkflowExecutionFailed(
            ChildWorkflowExecutionFailedEventAttributes {
                initiated_event_id,
                started_event_id,
                workflow_id,
                execution_id,
                failure,
                retry_policy: None,
                failed_time: None,
            },
        )))
    }

    fn map_child_workflow_timed_out(&self, attrs: &Value) -> OrcherResult<Option<Attributes>> {
        let initiated_event_id = attrs["initiated_event_id"].as_i64().unwrap_or(0);
        let started_event_id = attrs["started_event_id"].as_i64().unwrap_or(0);
        let workflow_id = attrs["child_workflow_id"]
            .as_str()
            .unwrap_or("unknown")
            .to_string();
        let execution_id = attrs["child_execution_id"]
            .as_str()
            .unwrap_or("unknown")
            .to_string();

        Ok(Some(Attributes::ChildWorkflowExecutionTimedOut(
            ChildWorkflowExecutionTimedOutEventAttributes {
                initiated_event_id,
                started_event_id,
                workflow_id,
                execution_id,
                timeout_type: 0, // TimeoutType::Unspecified
                retry_policy: None,
            },
        )))
    }

    // ========================================================================
    // UPDATE ATTRIBUTE MAPPERS
    // ========================================================================

    fn map_update_accepted(&self, attrs: &Value) -> OrcherResult<Option<Attributes>> {
        let update_id = attrs["update_id"].as_str().unwrap_or("").to_string();
        let update_type = attrs["update_type"].as_str().unwrap_or("").to_string();
        let args = attrs["args"]
            .as_str()
            .and_then(|s| {
                base64::Engine::decode(&base64::engine::general_purpose::STANDARD, s).ok()
            })
            .unwrap_or_default();
        let identity = attrs["identity"].as_str().unwrap_or("").to_string();

        Ok(Some(Attributes::UpdateAccepted(
            UpdateAcceptedEventAttributes {
                update_id,
                update_type,
                args,
                identity,
            },
        )))
    }

    fn map_update_completed(&self, attrs: &Value) -> OrcherResult<Option<Attributes>> {
        let update_id = attrs["update_id"].as_str().unwrap_or("").to_string();
        let update_type = attrs["update_type"].as_str().unwrap_or("").to_string();
        let result = attrs["result"]
            .as_str()
            .and_then(|s| {
                base64::Engine::decode(&base64::engine::general_purpose::STANDARD, s).ok()
            })
            .unwrap_or_default();
        let failure = attrs.get("failure").and_then(|f| {
            Some(Failure {
                message: f["message"].as_str()?.to_string(),
                ..Default::default()
            })
        });

        Ok(Some(Attributes::UpdateCompleted(
            UpdateCompletedEventAttributes {
                update_id,
                update_type,
                result,
                failure,
            },
        )))
    }

    fn map_event_received(&self, attrs: &Value) -> OrcherResult<Option<Attributes>> {
        let event_name = attrs["event_name"].as_str().unwrap_or("").to_string();
        let identity = attrs["identity"].as_str().unwrap_or("").to_string();
        let payload = attrs["payload"]
            .as_str()
            .and_then(|s| {
                base64::Engine::decode(&base64::engine::general_purpose::STANDARD, s).ok()
            })
            .unwrap_or_default();

        Ok(Some(Attributes::EventReceived(
            EventReceivedEventAttributes {
                event_name,
                payload,
                identity,
            },
        )))
    }
}

impl Default for JournalMapper {
    fn default() -> Self {
        Self::new()
    }
}

// ================================================================================================
// TESTS
// ================================================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use orcher_common::types::time::Timestamptz;
    use serde_json::json;

    #[test]
    fn test_map_entry_type_workflow_started() {
        let mapper = JournalMapper::new();
        let entry_type = mapper.map_entry_type("WorkflowExecutionStarted").unwrap();
        assert_eq!(entry_type, EntryType::WorkflowExecutionStarted);
    }

    #[test]
    fn test_map_entry_type_task_completed() {
        let mapper = JournalMapper::new();
        let entry_type = mapper.map_entry_type("TaskCompleted").unwrap();
        assert_eq!(entry_type, EntryType::TaskCompleted);
    }

    #[test]
    fn test_map_entry_type_child_workflow() {
        let mapper = JournalMapper::new();
        let entry_type = mapper
            .map_entry_type("ChildWorkflowExecutionStarted")
            .unwrap();
        assert_eq!(entry_type, EntryType::ChildWorkflowExecutionStarted);
    }

    #[test]
    fn test_map_entry_type_unknown() {
        let mapper = JournalMapper::new();
        let entry_type = mapper.map_entry_type("UnknownEventType").unwrap();
        assert_eq!(entry_type, EntryType::Unspecified);
    }

    #[test]
    fn test_to_proto_workflow_started() {
        let mapper = JournalMapper::new();

        let storage_entry = StorageJournalEntry {
            id: 1,
            workflow_id: "wf-123".to_string(),
            execution_id: "exec-456".to_string(),
            event_id: 1,
            event_type: "WorkflowExecutionStarted".to_string(),
            timestamp: Timestamptz::now(),
            payload: json!({
                "workflow_type": "OrderWorkflow",
                "task_queue": "orders",
                "input": {"order_id": "12345"}
            }),
            version: 1,
            task_id: 0,
            created_at: Timestamptz::now(),
            shard_id: None,
            organization_id: None,
        };

        let proto_entry = mapper.to_proto(&storage_entry).unwrap();

        assert_eq!(proto_entry.entry_id, 1);
        assert_eq!(
            proto_entry.entry_type,
            EntryType::WorkflowExecutionStarted as i32
        );
        assert!(proto_entry.attributes.is_some());
    }

    #[test]
    fn test_to_proto_child_workflow_completed() {
        let mapper = JournalMapper::new();

        let storage_entry = StorageJournalEntry {
            id: 1,
            workflow_id: "wf-123".to_string(),
            execution_id: "exec-456".to_string(),
            event_id: 5,
            event_type: "ChildWorkflowExecutionCompleted".to_string(),
            timestamp: Timestamptz::now(),
            payload: json!({
                "child_workflow_id": "child-wf-789",
                "child_execution_id": "child-exec-101",
                "child_workflow_type": "InventoryWorkflow",
                "initiated_event_id": 3,
                "started_event_id": 4,
                "result": {"status": "success"}
            }),
            version: 1,
            task_id: 0,
            created_at: Timestamptz::now(),
            shard_id: None,
            organization_id: None,
        };

        let proto_entry = mapper.to_proto(&storage_entry).unwrap();

        assert_eq!(proto_entry.entry_id, 5);
        assert_eq!(
            proto_entry.entry_type,
            EntryType::ChildWorkflowExecutionCompleted as i32
        );
        assert!(proto_entry.attributes.is_some());

        if let Some(Attributes::ChildWorkflowExecutionCompleted(attrs)) = proto_entry.attributes {
            assert_eq!(attrs.workflow_id, "child-wf-789");
            assert_eq!(attrs.execution_id, "child-exec-101");
        }
    }

    // ========================================================================
    // Task #9: Comprehensive Tests for All Entry Types
    // ========================================================================

    #[test]
    fn test_map_all_workflow_entry_types() {
        // Task #9: Test mapping of all workflow lifecycle entry types
        let mapper = JournalMapper::new();

        let workflow_types = vec![
            (
                "WorkflowExecutionStarted",
                EntryType::WorkflowExecutionStarted,
            ),
            (
                "WorkflowExecutionCompleted",
                EntryType::WorkflowExecutionCompleted,
            ),
            (
                "WorkflowExecutionFailed",
                EntryType::WorkflowExecutionFailed,
            ),
            (
                "WorkflowExecutionTimedOut",
                EntryType::WorkflowExecutionTimedOut,
            ),
            (
                "WorkflowExecutionCanceled",
                EntryType::WorkflowExecutionCanceled,
            ),
            (
                "WorkflowExecutionRestartedFresh",
                EntryType::WorkflowExecutionRestartedFresh,
            ),
            (
                "WorkflowExecutionCancelRequested",
                EntryType::WorkflowExecutionCancelRequested,
            ),
            (
                "WorkflowExecutionTerminated",
                EntryType::WorkflowExecutionTerminated,
            ),
        ];

        for (type_str, expected) in workflow_types {
            let result = mapper.map_entry_type(type_str).unwrap();
            assert_eq!(result, expected, "Failed to map {}", type_str);
        }
    }

    #[test]
    fn test_map_all_task_entry_types() {
        // Task #9: Test mapping of all task entry types
        let mapper = JournalMapper::new();

        let task_types = vec![
            ("TaskScheduled", EntryType::TaskScheduled),
            ("TaskStarted", EntryType::TaskStarted),
            ("TaskCompleted", EntryType::TaskCompleted),
            ("TaskFailed", EntryType::TaskFailed),
            ("TaskTimedOut", EntryType::TaskTimedOut),
            ("TaskCancelRequested", EntryType::TaskCancelRequested),
            ("TaskCanceled", EntryType::TaskCanceled),
        ];

        for (type_str, expected) in task_types {
            let result = mapper.map_entry_type(type_str).unwrap();
            assert_eq!(result, expected, "Failed to map {}", type_str);
        }
    }

    #[test]
    fn test_map_all_timer_entry_types() {
        // Task #9: Test mapping of all timer entry types
        let mapper = JournalMapper::new();

        let timer_types = vec![
            ("TimerStarted", EntryType::TimerStarted),
            ("TimerFired", EntryType::TimerFired),
            ("TimerCanceled", EntryType::TimerCanceled),
        ];

        for (type_str, expected) in timer_types {
            let result = mapper.map_entry_type(type_str).unwrap();
            assert_eq!(result, expected, "Failed to map {}", type_str);
        }
    }

    #[test]
    fn test_map_all_child_workflow_entry_types() {
        // Task #9: Test mapping of all child workflow entry types
        let mapper = JournalMapper::new();

        let child_types = vec![
            (
                "ChildWorkflowExecutionStarted",
                EntryType::ChildWorkflowExecutionStarted,
            ),
            (
                "ChildWorkflowExecutionCompleted",
                EntryType::ChildWorkflowExecutionCompleted,
            ),
            (
                "ChildWorkflowExecutionFailed",
                EntryType::ChildWorkflowExecutionFailed,
            ),
            (
                "ChildWorkflowExecutionTimedOut",
                EntryType::ChildWorkflowExecutionTimedOut,
            ),
            (
                "ChildWorkflowExecutionCanceled",
                EntryType::ChildWorkflowExecutionCanceled,
            ),
            (
                "ChildWorkflowExecutionTerminated",
                EntryType::ChildWorkflowExecutionTerminated,
            ),
        ];

        for (type_str, expected) in child_types {
            let result = mapper.map_entry_type(type_str).unwrap();
            assert_eq!(result, expected, "Failed to map {}", type_str);
        }
    }

    #[test]
    fn test_to_proto_workflow_completed() {
        // Task #9: Test WorkflowExecutionCompleted mapping
        let mapper = JournalMapper::new();

        let storage_entry = StorageJournalEntry {
            id: 1,
            workflow_id: "wf-123".to_string(),
            execution_id: "exec-456".to_string(),
            event_id: 10,
            event_type: "WorkflowExecutionCompleted".to_string(),
            timestamp: Timestamptz::now(),
            payload: json!({
                "result": {"status": "success", "total": 100}
            }),
            version: 1,
            task_id: 0,
            created_at: Timestamptz::now(),
            shard_id: None,
            organization_id: None,
        };

        let proto_entry = mapper.to_proto(&storage_entry).unwrap();

        assert_eq!(proto_entry.entry_id, 10);
        assert_eq!(
            proto_entry.entry_type,
            EntryType::WorkflowExecutionCompleted as i32
        );
        assert!(proto_entry.attributes.is_some());

        if let Some(Attributes::WorkflowExecutionCompleted(attrs)) = proto_entry.attributes {
            assert!(!attrs.result.is_empty());
        } else {
            panic!("Expected WorkflowExecutionCompleted attributes");
        }
    }

    #[test]
    fn test_to_proto_workflow_failed() {
        // Task #9: Test WorkflowExecutionFailed mapping
        let mapper = JournalMapper::new();

        let storage_entry = StorageJournalEntry {
            id: 1,
            workflow_id: "wf-123".to_string(),
            execution_id: "exec-456".to_string(),
            event_id: 10,
            event_type: "WorkflowExecutionFailed".to_string(),
            timestamp: Timestamptz::now(),
            payload: json!({
                "failure": {
                    "message": "Task execution failed",
                    "source": "TaskFailed",
                    "stack_trace": "at line 42"
                }
            }),
            version: 1,
            task_id: 0,
            created_at: Timestamptz::now(),
            shard_id: None,
            organization_id: None,
        };

        let proto_entry = mapper.to_proto(&storage_entry).unwrap();

        assert_eq!(
            proto_entry.entry_type,
            EntryType::WorkflowExecutionFailed as i32
        );

        if let Some(Attributes::WorkflowExecutionFailed(attrs)) = proto_entry.attributes {
            assert!(attrs.failure.is_some());
            let failure = attrs.failure.unwrap();
            assert_eq!(failure.message, "Task execution failed");
        } else {
            panic!("Expected WorkflowExecutionFailed attributes");
        }
    }

    #[test]
    fn test_to_proto_task_scheduled() {
        // Task #9: Test TaskScheduled mapping
        let mapper = JournalMapper::new();

        let storage_entry = StorageJournalEntry {
            id: 1,
            workflow_id: "wf-123".to_string(),
            execution_id: "exec-456".to_string(),
            event_id: 3,
            event_type: "TaskScheduled".to_string(),
            timestamp: Timestamptz::now(),
            payload: json!({
                "task_id": "task-1",
                "task_type": "process_payment",
                "input": {"amount": 99.99},
                "schedule_to_close_timeout": 300,
                "start_to_close_timeout": 60
            }),
            version: 1,
            task_id: 1,
            created_at: Timestamptz::now(),
            shard_id: None,
            organization_id: None,
        };

        let proto_entry = mapper.to_proto(&storage_entry).unwrap();

        assert_eq!(proto_entry.entry_type, EntryType::TaskScheduled as i32);
        assert_eq!(proto_entry.task_id, 1);

        if let Some(Attributes::TaskScheduled(attrs)) = proto_entry.attributes {
            assert_eq!(attrs.task_id, "task-1");
            assert_eq!(attrs.task_type, "process_payment");
            assert!(!attrs.input.is_empty());
        } else {
            panic!("Expected TaskScheduled attributes");
        }
    }

    #[test]
    fn test_to_proto_task_started() {
        // Task #9: Test TaskStarted mapping
        let mapper = JournalMapper::new();

        let storage_entry = StorageJournalEntry {
            id: 1,
            workflow_id: "wf-123".to_string(),
            execution_id: "exec-456".to_string(),
            event_id: 4,
            event_type: "TaskStarted".to_string(),
            timestamp: Timestamptz::now(),
            payload: json!({
                "scheduled_event_id": 3,
                "identity": "service-worker-1"
            }),
            version: 1,
            task_id: 1,
            created_at: Timestamptz::now(),
            shard_id: None,
            organization_id: None,
        };

        let proto_entry = mapper.to_proto(&storage_entry).unwrap();

        assert_eq!(proto_entry.entry_type, EntryType::TaskStarted as i32);

        if let Some(Attributes::TaskStarted(attrs)) = proto_entry.attributes {
            assert_eq!(attrs.scheduled_event_id, 3);
            assert_eq!(attrs.identity, "service-worker-1");
        } else {
            panic!("Expected TaskStarted attributes");
        }
    }

    #[test]
    fn test_to_proto_task_completed() {
        // Task #9: Test TaskCompleted mapping
        let mapper = JournalMapper::new();

        let storage_entry = StorageJournalEntry {
            id: 1,
            workflow_id: "wf-123".to_string(),
            execution_id: "exec-456".to_string(),
            event_id: 5,
            event_type: "TaskCompleted".to_string(),
            timestamp: Timestamptz::now(),
            payload: json!({
                "scheduled_event_id": 3,
                "started_event_id": 4,
                "result": {"payment_id": "pay_123", "status": "success"}
            }),
            version: 1,
            task_id: 1,
            created_at: Timestamptz::now(),
            shard_id: None,
            organization_id: None,
        };

        let proto_entry = mapper.to_proto(&storage_entry).unwrap();

        assert_eq!(proto_entry.entry_type, EntryType::TaskCompleted as i32);

        if let Some(Attributes::TaskCompleted(attrs)) = proto_entry.attributes {
            assert_eq!(attrs.scheduled_event_id, 3);
            assert_eq!(attrs.started_event_id, 4);
            assert!(!attrs.result.is_empty());
        } else {
            panic!("Expected TaskCompleted attributes");
        }
    }

    #[test]
    fn test_to_proto_task_failed() {
        // Task #9: Test TaskFailed mapping
        let mapper = JournalMapper::new();

        let storage_entry = StorageJournalEntry {
            id: 1,
            workflow_id: "wf-123".to_string(),
            execution_id: "exec-456".to_string(),
            event_id: 5,
            event_type: "TaskFailed".to_string(),
            timestamp: Timestamptz::now(),
            payload: json!({
                "scheduled_event_id": 3,
                "started_event_id": 4,
                "failure": {
                    "message": "Payment declined",
                    "source": "PaymentGateway"
                }
            }),
            version: 1,
            task_id: 1,
            created_at: Timestamptz::now(),
            shard_id: None,
            organization_id: None,
        };

        let proto_entry = mapper.to_proto(&storage_entry).unwrap();

        assert_eq!(proto_entry.entry_type, EntryType::TaskFailed as i32);

        if let Some(Attributes::TaskFailed(attrs)) = proto_entry.attributes {
            assert_eq!(attrs.scheduled_event_id, 3);
            assert_eq!(attrs.started_event_id, 4);
            assert!(attrs.failure.is_some());
            let failure = attrs.failure.unwrap();
            assert_eq!(failure.message, "Payment declined");
        } else {
            panic!("Expected TaskFailed attributes");
        }
    }

    #[test]
    fn test_to_proto_timer_started() {
        // Task #9: Test TimerStarted mapping
        let mapper = JournalMapper::new();

        let storage_entry = StorageJournalEntry {
            id: 1,
            workflow_id: "wf-123".to_string(),
            execution_id: "exec-456".to_string(),
            event_id: 6,
            event_type: "TimerStarted".to_string(),
            timestamp: Timestamptz::now(),
            payload: json!({
                "timer_id": "timer-1",
                "start_to_fire_timeout": 60
            }),
            version: 1,
            task_id: 0,
            created_at: Timestamptz::now(),
            shard_id: None,
            organization_id: None,
        };

        let proto_entry = mapper.to_proto(&storage_entry).unwrap();

        assert_eq!(proto_entry.entry_type, EntryType::TimerStarted as i32);

        if let Some(Attributes::TimerStarted(attrs)) = proto_entry.attributes {
            assert_eq!(attrs.timer_id, "timer-1");
            assert!(attrs.start_to_fire_timeout.is_some());
        } else {
            panic!("Expected TimerStarted attributes");
        }
    }

    #[test]
    fn test_to_proto_timer_fired() {
        // Task #9: Test TimerFired mapping
        let mapper = JournalMapper::new();

        let storage_entry = StorageJournalEntry {
            id: 1,
            workflow_id: "wf-123".to_string(),
            execution_id: "exec-456".to_string(),
            event_id: 7,
            event_type: "TimerFired".to_string(),
            timestamp: Timestamptz::now(),
            payload: json!({
                "timer_id": "timer-1",
                "started_event_id": 6
            }),
            version: 1,
            task_id: 0,
            created_at: Timestamptz::now(),
            shard_id: None,
            organization_id: None,
        };

        let proto_entry = mapper.to_proto(&storage_entry).unwrap();

        assert_eq!(proto_entry.entry_type, EntryType::TimerFired as i32);

        if let Some(Attributes::TimerFired(attrs)) = proto_entry.attributes {
            assert_eq!(attrs.timer_id, "timer-1");
            assert_eq!(attrs.started_event_id, 6);
        } else {
            panic!("Expected TimerFired attributes");
        }
    }

    #[test]
    fn test_to_proto_timer_canceled() {
        // Task #9: Test TimerCanceled mapping
        let mapper = JournalMapper::new();

        let storage_entry = StorageJournalEntry {
            id: 1,
            workflow_id: "wf-123".to_string(),
            execution_id: "exec-456".to_string(),
            event_id: 8,
            event_type: "TimerCanceled".to_string(),
            timestamp: Timestamptz::now(),
            payload: json!({
                "timer_id": "timer-1",
                "started_event_id": 6
            }),
            version: 1,
            task_id: 0,
            created_at: Timestamptz::now(),
            shard_id: None,
            organization_id: None,
        };

        let proto_entry = mapper.to_proto(&storage_entry).unwrap();

        assert_eq!(proto_entry.entry_type, EntryType::TimerCanceled as i32);

        if let Some(Attributes::TimerCanceled(attrs)) = proto_entry.attributes {
            assert_eq!(attrs.timer_id, "timer-1");
            assert_eq!(attrs.started_event_id, 6);
        } else {
            panic!("Expected TimerCanceled attributes");
        }
    }

    #[test]
    fn test_to_proto_child_workflow_started() {
        // Task #9: Test ChildWorkflowExecutionStarted mapping
        let mapper = JournalMapper::new();

        let storage_entry = StorageJournalEntry {
            id: 1,
            workflow_id: "wf-123".to_string(),
            execution_id: "exec-456".to_string(),
            event_id: 9,
            event_type: "ChildWorkflowExecutionStarted".to_string(),
            timestamp: Timestamptz::now(),
            payload: json!({
                "child_workflow_id": "child-wf-789",
                "child_execution_id": "child-exec-101",
                "child_workflow_type": "InventoryWorkflow",
                "initiated_event_id": 8
            }),
            version: 1,
            task_id: 0,
            created_at: Timestamptz::now(),
            shard_id: None,
            organization_id: None,
        };

        let proto_entry = mapper.to_proto(&storage_entry).unwrap();

        assert_eq!(
            proto_entry.entry_type,
            EntryType::ChildWorkflowExecutionStarted as i32
        );

        if let Some(Attributes::ChildWorkflowExecutionStarted(attrs)) = proto_entry.attributes {
            assert_eq!(attrs.workflow_id, "child-wf-789");
            assert_eq!(attrs.execution_id, "child-exec-101");
            assert_eq!(attrs.workflow_type, "InventoryWorkflow");
        } else {
            panic!("Expected ChildWorkflowExecutionStarted attributes");
        }
    }

    /// A child's start reaches workers as the child's start, with its
    /// workflow id, type and run id, rather than as an entry of no type with
    /// nothing in it. Entries journaled before the run id was recorded keep
    /// an empty one.
    #[test]
    fn a_child_initiation_reaches_workers_as_its_start() {
        let mapper = JournalMapper::new();
        let entry = |payload| StorageJournalEntry {
            id: 1,
            workflow_id: "wf-123".to_string(),
            execution_id: "exec-456".to_string(),
            event_id: 7,
            event_type: "ChildWorkflowExecutionInitiated".to_string(),
            timestamp: Timestamptz::now(),
            payload,
            version: 1,
            task_id: 0,
            created_at: Timestamptz::now(),
            shard_id: None,
            organization_id: None,
        };

        for (payload, run_id) in [
            (
                json!({
                    "child_workflow_id": "order-7-child-2",
                    "child_workflow_type": "ShipOrder",
                    "child_execution_id": "run-9",
                    "task_queue": "orders",
                    "input": {}
                }),
                "run-9",
            ),
            (
                json!({
                    "child_workflow_id": "order-7-child-2",
                    "child_workflow_type": "ShipOrder",
                    "task_queue": "orders",
                    "input": {}
                }),
                "",
            ),
        ] {
            let proto = mapper.to_proto(&entry(payload)).unwrap();
            assert_eq!(
                proto.entry_type,
                EntryType::ChildWorkflowExecutionStarted as i32
            );
            let Some(Attributes::ChildWorkflowExecutionStarted(attrs)) = proto.attributes else {
                panic!("no child start attributes: {:?}", proto.attributes);
            };
            assert_eq!(attrs.workflow_id, "order-7-child-2");
            assert_eq!(attrs.workflow_type, "ShipOrder");
            assert_eq!(attrs.execution_id, run_id);
            assert_eq!(attrs.initiated_event_id, 7);
        }
    }

    /// A cancelled child is reported as a failure; the failure type is how
    /// the parent tells that apart from the child failing.
    #[test]
    fn a_child_failure_carries_its_failure_type() {
        let mapper = JournalMapper::new();
        let entry = |payload| StorageJournalEntry {
            id: 1,
            workflow_id: "wf-123".to_string(),
            execution_id: "exec-456".to_string(),
            event_id: 11,
            event_type: "ChildWorkflowExecutionFailed".to_string(),
            timestamp: Timestamptz::now(),
            payload,
            version: 1,
            task_id: 0,
            created_at: Timestamptz::now(),
            shard_id: None,
            organization_id: None,
        };
        let failure_type = |payload| match mapper.to_proto(&entry(payload)).unwrap().attributes {
            Some(Attributes::ChildWorkflowExecutionFailed(attrs)) => {
                attrs.failure.expect("failure").failure_type
            }
            other => panic!("expected ChildWorkflowExecutionFailed, got {other:?}"),
        };

        assert_eq!(
            failure_type(json!({
                "child_workflow_id": "child_0",
                "failure_reason": "cancelled",
                "failure_type": "ChildWorkflowCanceled",
            })),
            "ChildWorkflowCanceled"
        );
        // Entries written before the field existed read as untyped failures.
        assert_eq!(
            failure_type(json!({ "child_workflow_id": "child_0", "failure_reason": "boom" })),
            ""
        );
    }

    #[test]
    fn test_to_proto_child_workflow_failed() {
        // Task #9: Test ChildWorkflowExecutionFailed mapping
        let mapper = JournalMapper::new();

        let storage_entry = StorageJournalEntry {
            id: 1,
            workflow_id: "wf-123".to_string(),
            execution_id: "exec-456".to_string(),
            event_id: 11,
            event_type: "ChildWorkflowExecutionFailed".to_string(),
            timestamp: Timestamptz::now(),
            payload: json!({
                "child_workflow_id": "child-wf-789",
                "child_execution_id": "child-exec-101",
                "child_workflow_type": "InventoryWorkflow",
                "initiated_event_id": 8,
                "started_event_id": 9,
                "failure": {
                    "message": "Inventory check failed",
                    "source": "InventoryService"
                }
            }),
            version: 1,
            task_id: 0,
            created_at: Timestamptz::now(),
            shard_id: None,
            organization_id: None,
        };

        let proto_entry = mapper.to_proto(&storage_entry).unwrap();

        assert_eq!(
            proto_entry.entry_type,
            EntryType::ChildWorkflowExecutionFailed as i32
        );

        if let Some(Attributes::ChildWorkflowExecutionFailed(attrs)) = proto_entry.attributes {
            assert_eq!(attrs.workflow_id, "child-wf-789");
            assert!(attrs.failure.is_some());
        } else {
            panic!("Expected ChildWorkflowExecutionFailed attributes");
        }
    }

    #[test]
    fn test_to_proto_child_workflow_timed_out() {
        // Task #9: Test ChildWorkflowExecutionTimedOut mapping
        let mapper = JournalMapper::new();

        let storage_entry = StorageJournalEntry {
            id: 1,
            workflow_id: "wf-123".to_string(),
            execution_id: "exec-456".to_string(),
            event_id: 11,
            event_type: "ChildWorkflowExecutionTimedOut".to_string(),
            timestamp: Timestamptz::now(),
            payload: json!({
                "child_workflow_id": "child-wf-789",
                "child_execution_id": "child-exec-101",
                "child_workflow_type": "InventoryWorkflow",
                "initiated_event_id": 8,
                "started_event_id": 9
            }),
            version: 1,
            task_id: 0,
            created_at: Timestamptz::now(),
            shard_id: None,
            organization_id: None,
        };

        let proto_entry = mapper.to_proto(&storage_entry).unwrap();

        assert_eq!(
            proto_entry.entry_type,
            EntryType::ChildWorkflowExecutionTimedOut as i32
        );

        if let Some(Attributes::ChildWorkflowExecutionTimedOut(attrs)) = proto_entry.attributes {
            assert_eq!(attrs.workflow_id, "child-wf-789");
            assert_eq!(attrs.execution_id, "child-exec-101");
        } else {
            panic!("Expected ChildWorkflowExecutionTimedOut attributes");
        }
    }

    #[test]
    fn test_to_proto_vec_multiple_entries() {
        // Task #9: Test converting multiple entries at once
        let mapper = JournalMapper::new();

        let entries = vec![
            StorageJournalEntry {
                id: 1,
                workflow_id: "wf-123".to_string(),
                execution_id: "exec-456".to_string(),
                event_id: 1,
                event_type: "WorkflowExecutionStarted".to_string(),
                timestamp: Timestamptz::now(),
                payload: json!({"workflow_type": "OrderWorkflow", "task_queue": "orders", "input": {}}),
                version: 1,
                task_id: 0,
                created_at: Timestamptz::now(),
                shard_id: None,
                organization_id: None,
            },
            StorageJournalEntry {
                id: 2,
                workflow_id: "wf-123".to_string(),
                execution_id: "exec-456".to_string(),
                event_id: 2,
                event_type: "TaskScheduled".to_string(),
                timestamp: Timestamptz::now(),
                payload: json!({"task_id": "task-1", "task_type": "process", "input": {}}),
                version: 1,
                task_id: 1,
                created_at: Timestamptz::now(),
                shard_id: None,
                organization_id: None,
            },
            StorageJournalEntry {
                id: 3,
                workflow_id: "wf-123".to_string(),
                execution_id: "exec-456".to_string(),
                event_id: 3,
                event_type: "TaskCompleted".to_string(),
                timestamp: Timestamptz::now(),
                payload: json!({"scheduled_event_id": 2, "started_event_id": 2, "result": {}}),
                version: 1,
                task_id: 1,
                created_at: Timestamptz::now(),
                shard_id: None,
                organization_id: None,
            },
        ];

        let proto_entries = mapper.to_proto_vec(&entries).unwrap();

        assert_eq!(proto_entries.len(), 3);
        assert_eq!(proto_entries[0].entry_id, 1);
        assert_eq!(proto_entries[1].entry_id, 2);
        assert_eq!(proto_entries[2].entry_id, 3);
    }

    #[test]
    fn test_to_proto_preserves_timestamp() {
        // Task #9: Test that timestamps are correctly converted
        let mapper = JournalMapper::new();

        let timestamp = Timestamptz::now();
        let storage_entry = StorageJournalEntry {
            id: 1,
            workflow_id: "wf-123".to_string(),
            execution_id: "exec-456".to_string(),
            event_id: 1,
            event_type: "WorkflowExecutionStarted".to_string(),
            timestamp,
            payload: json!({"workflow_type": "test", "task_queue": "test", "input": {}}),
            version: 1,
            task_id: 0,
            created_at: Timestamptz::now(),
            shard_id: None,
            organization_id: None,
        };

        let proto_entry = mapper.to_proto(&storage_entry).unwrap();

        assert!(proto_entry.timestamp.is_some());
        let proto_ts = proto_entry.timestamp.unwrap();
        assert_eq!(proto_ts.seconds, timestamp.timestamp());
    }

    #[test]
    fn test_to_proto_preserves_task_id() {
        // Task #9: Test that task_id is correctly preserved
        let mapper = JournalMapper::new();

        let storage_entry = StorageJournalEntry {
            id: 1,
            workflow_id: "wf-123".to_string(),
            execution_id: "exec-456".to_string(),
            event_id: 5,
            event_type: "TaskCompleted".to_string(),
            timestamp: Timestamptz::now(),
            payload: json!({"task_id": 42, "scheduled_event_id": 3, "started_event_id": 4, "result": {}}),
            version: 1,
            task_id: 42,
            created_at: Timestamptz::now(),
            shard_id: None,
            organization_id: None,
        };

        let proto_entry = mapper.to_proto(&storage_entry).unwrap();

        assert_eq!(proto_entry.task_id, 42);
    }

    #[test]
    fn test_to_proto_preserves_version() {
        // Task #9: Test that version is correctly preserved
        let mapper = JournalMapper::new();

        let storage_entry = StorageJournalEntry {
            id: 1,
            workflow_id: "wf-123".to_string(),
            execution_id: "exec-456".to_string(),
            event_id: 1,
            event_type: "WorkflowExecutionStarted".to_string(),
            timestamp: Timestamptz::now(),
            payload: json!({"workflow_type": "test", "task_queue": "test", "input": {}}),
            version: 5,
            task_id: 0,
            created_at: Timestamptz::now(),
            shard_id: None,
            organization_id: None,
        };

        let proto_entry = mapper.to_proto(&storage_entry).unwrap();

        assert_eq!(proto_entry.version, 5);
    }

    #[test]
    fn test_mapper_is_reusable() {
        // Task #9: Test that the same mapper can be used multiple times
        let mapper = JournalMapper::new();

        let entry1 = StorageJournalEntry {
            id: 1,
            workflow_id: "wf-1".to_string(),
            execution_id: "exec-1".to_string(),
            event_id: 1,
            event_type: "WorkflowExecutionStarted".to_string(),
            timestamp: Timestamptz::now(),
            payload: json!({"workflow_type": "test", "task_queue": "test", "input": {}}),
            version: 1,
            task_id: 0,
            created_at: Timestamptz::now(),
            shard_id: None,
            organization_id: None,
        };

        let entry2 = StorageJournalEntry {
            id: 2,
            workflow_id: "wf-2".to_string(),
            execution_id: "exec-2".to_string(),
            event_id: 1,
            event_type: "TaskScheduled".to_string(),
            timestamp: Timestamptz::now(),
            payload: json!({"task_id": 1, "task_name": "test", "input": {}}),
            version: 1,
            task_id: 1,
            created_at: Timestamptz::now(),
            shard_id: None,
            organization_id: None,
        };

        let proto1 = mapper.to_proto(&entry1).unwrap();
        let proto2 = mapper.to_proto(&entry2).unwrap();

        assert_eq!(
            proto1.entry_type,
            EntryType::WorkflowExecutionStarted as i32
        );
        assert_eq!(proto2.entry_type, EntryType::TaskScheduled as i32);
    }

    #[test]
    fn test_to_proto_vec() {
        let mapper = JournalMapper::new();

        let entries = vec![
            StorageJournalEntry {
                id: 1,
                workflow_id: "wf-123".to_string(),
                execution_id: "exec-456".to_string(),
                event_id: 1,
                event_type: "WorkflowExecutionStarted".to_string(),
                timestamp: Timestamptz::now(),
                payload: json!({}),
                version: 1,
                task_id: 0,
                created_at: Timestamptz::now(),
                shard_id: None,
                organization_id: None,
            },
            StorageJournalEntry {
                id: 2,
                workflow_id: "wf-123".to_string(),
                execution_id: "exec-456".to_string(),
                event_id: 2,
                event_type: "TaskScheduled".to_string(),
                timestamp: Timestamptz::now(),
                payload: json!({
                    "task_id": "task-1",
                    "task_type": "ProcessOrder",
                    "task_queue": "orders",
                    "input": {}
                }),
                version: 1,
                task_id: 1,
                created_at: Timestamptz::now(),
                shard_id: None,
                organization_id: None,
            },
        ];

        let proto_entries = mapper.to_proto_vec(&entries).unwrap();

        assert_eq!(proto_entries.len(), 2);
        assert_eq!(proto_entries[0].entry_id, 1);
        assert_eq!(proto_entries[1].entry_id, 2);
    }
}
