//! Journal entry creation and querying.

use base64::Engine;
use orcher_common::OrcherResult;
use orcher_storage::repositories::domains::journal::JournalRepository;
use orcher_storage::schema::types::journal::JournalEntry;
use std::sync::Arc;
use tracing::{debug, info};

/// High-level service for journal entry operations
///
/// Provides a clean API on top of JournalRepository with automatic
/// event ID sequencing and type-safe methods for each event type.
pub struct JournalService {
    /// Underlying journal repository
    repository: Arc<JournalRepository>,
}

impl JournalService {
    /// Create a new journal service
    ///
    /// # Arguments
    ///
    /// * `repository` - The journal repository to use
    pub fn new(repository: Arc<JournalRepository>) -> Self {
        Self { repository }
    }

    /// Append a journal entry with automatic event ID generation
    ///
    /// This is the low-level method used by all high-level append methods.
    ///
    /// # Arguments
    ///
    /// * `workflow_id` - The workflow identifier
    /// * `execution_id` - The execution identifier
    /// * `event_type` - The type of event
    /// * `payload` - The event payload as JSON
    ///
    /// # Returns
    ///
    /// The event ID of the appended entry
    pub async fn append_entry(
        &self,
        workflow_id: &str,
        execution_id: &str,
        event_type: &str,
        payload: serde_json::Value,
    ) -> OrcherResult<i64> {
        // Allocate the event id and insert in one transaction. Allocating
        // first and appending separately let two concurrent appends to one
        // execution take the same id; see JournalRepository::append_next.
        let entry = JournalEntry::new(
            workflow_id.to_string(),
            execution_id.to_string(),
            0,
            event_type.to_string(),
            payload,
        );
        let created = self.repository.append_next(entry).await?;

        debug!(
            workflow_id = %workflow_id,
            execution_id = %execution_id,
            event_id = created.event_id,
            event_type = %event_type,
            "Journal entry appended"
        );

        Ok(created.event_id)
    }

    /// Append a journal entry with a specific task ID
    ///
    /// # Arguments
    ///
    /// * `workflow_id` - The workflow identifier
    /// * `execution_id` - The execution identifier
    /// * `event_type` - The type of event
    /// * `payload` - The event payload as JSON
    /// * `task_id` - The task ID to associate with this entry
    pub async fn append_entry_with_task(
        &self,
        workflow_id: &str,
        execution_id: &str,
        event_type: &str,
        payload: serde_json::Value,
        task_id: i64,
    ) -> OrcherResult<i64> {
        // One transaction for allocation and insert; see append_entry.
        let entry = JournalEntry::new(
            workflow_id.to_string(),
            execution_id.to_string(),
            0,
            event_type.to_string(),
            payload,
        )
        .with_task_id(task_id);
        let created = self.repository.append_next(entry).await?;

        Ok(created.event_id)
    }

    // ========================================================================
    // WORKFLOW LIFECYCLE EVENTS
    // ========================================================================

    /// Append WorkflowExecutionStarted event
    ///
    /// # Arguments
    ///
    /// * `workflow_id` - The workflow identifier
    /// * `execution_id` - The execution identifier
    /// * `payload` - Event payload (input, workflow_type, etc.)
    pub async fn append_workflow_started(
        &self,
        workflow_id: &str,
        execution_id: &str,
        payload: serde_json::Value,
    ) -> OrcherResult<i64> {
        info!(
            workflow_id = %workflow_id,
            execution_id = %execution_id,
            "Appending WorkflowExecutionStarted event"
        );

        self.append_entry(
            workflow_id,
            execution_id,
            "WorkflowExecutionStarted",
            payload,
        )
        .await
    }

    /// Append WorkflowExecutionCompleted event
    ///
    /// # Arguments
    ///
    /// * `workflow_id` - The workflow identifier
    /// * `execution_id` - The execution identifier
    /// * `result` - The workflow result
    pub async fn append_workflow_completed(
        &self,
        workflow_id: &str,
        execution_id: &str,
        result: serde_json::Value,
    ) -> OrcherResult<i64> {
        info!(
            workflow_id = %workflow_id,
            execution_id = %execution_id,
            "Appending WorkflowExecutionCompleted event"
        );

        let payload = serde_json::json!({
            "result": result,
        });

        self.append_entry(
            workflow_id,
            execution_id,
            "WorkflowExecutionCompleted",
            payload,
        )
        .await
    }

    /// Append WorkflowExecutionFailed event
    ///
    /// # Arguments
    ///
    /// * `workflow_id` - The workflow identifier
    /// * `execution_id` - The execution identifier
    /// * `failure_reason` - Reason for failure
    /// * `failure_details` - Optional detailed failure information
    pub async fn append_workflow_failed(
        &self,
        workflow_id: &str,
        execution_id: &str,
        failure_reason: String,
        failure_details: Option<serde_json::Value>,
    ) -> OrcherResult<i64> {
        info!(
            workflow_id = %workflow_id,
            execution_id = %execution_id,
            failure_reason = %failure_reason,
            "Appending WorkflowExecutionFailed event"
        );

        let payload = serde_json::json!({
            "failure_reason": failure_reason,
            "failure_details": failure_details,
        });

        self.append_entry(
            workflow_id,
            execution_id,
            "WorkflowExecutionFailed",
            payload,
        )
        .await
    }

    /// Append WorkflowExecutionCanceled event
    ///
    /// # Arguments
    ///
    /// * `workflow_id` - The workflow identifier
    /// * `execution_id` - The execution identifier
    /// * `reason` - Reason for cancellation
    pub async fn append_workflow_canceled(
        &self,
        workflow_id: &str,
        execution_id: &str,
        reason: String,
    ) -> OrcherResult<i64> {
        info!(
            workflow_id = %workflow_id,
            execution_id = %execution_id,
            "Appending WorkflowExecutionCanceled event"
        );

        let payload = serde_json::json!({
            "reason": reason,
        });

        self.append_entry(
            workflow_id,
            execution_id,
            "WorkflowExecutionCanceled",
            payload,
        )
        .await
    }

    /// Append the request to cancel a running workflow, once.
    ///
    /// The workflow observes it on its next activation and may clean up
    /// before it ends. Recorded at most once per execution, under the
    /// per-execution lock, so a cancellation retried after a partial failure,
    /// or two racing each other, leave a single entry.
    ///
    /// Returns `true` when this call appended it.
    pub async fn append_workflow_cancel_requested(
        &self,
        workflow_id: &str,
        execution_id: &str,
        reason: &str,
    ) -> OrcherResult<bool> {
        let entry = JournalEntry::new(
            workflow_id.to_string(),
            execution_id.to_string(),
            0,
            "WorkflowExecutionCancelRequested".to_string(),
            serde_json::json!({ "request": "cancel", "reason": reason }),
        );
        let appended = self
            .repository
            .append_next_once(
                entry,
                &["WorkflowExecutionCancelRequested"],
                "request",
                "cancel",
            )
            .await?;
        Ok(appended.is_some())
    }

    /// Append `step`'s `StepCompleted` entry to execution `execution_id`'s
    /// journal, unless it already holds one for the step: a step settles
    /// once, and the first outcome recorded stands. Returns whether this call
    /// appended it.
    pub async fn append_settled_step(
        &self,
        workflow_id: &str,
        execution_id: &str,
        step: &super::SettledStep,
    ) -> OrcherResult<bool> {
        let appended = self
            .repository
            .append_next_once(
                step.journal_entry(workflow_id, execution_id),
                &[super::STEP_COMPLETED],
                "step_name",
                &step.step_name,
            )
            .await?;
        Ok(appended.is_some())
    }

    /// Append WorkflowExecutionTimedOut event
    ///
    /// # Arguments
    ///
    /// * `workflow_id` - The workflow identifier
    /// * `execution_id` - The execution identifier
    /// * `timeout_duration_ms` - The timeout duration in milliseconds
    pub async fn append_workflow_timed_out(
        &self,
        workflow_id: &str,
        execution_id: &str,
        timeout_duration_ms: i64,
    ) -> OrcherResult<i64> {
        info!(
            workflow_id = %workflow_id,
            execution_id = %execution_id,
            "Appending WorkflowExecutionTimedOut event"
        );

        let payload = serde_json::json!({
            "timeout_duration_ms": timeout_duration_ms,
        });

        self.append_entry(
            workflow_id,
            execution_id,
            "WorkflowExecutionTimedOut",
            payload,
        )
        .await
    }

    /// Append WorkflowExecutionRestartedFresh event
    ///
    /// Records that a workflow execution was restarted with fresh state
    /// via `restart_fresh()`, discarding existing history and beginning a new execution.
    pub async fn append_workflow_restarted_fresh(
        &self,
        workflow_id: &str,
        execution_id: &str,
        new_execution_id: &str,
        payload: serde_json::Value,
    ) -> OrcherResult<i64> {
        info!(
            workflow_id = %workflow_id,
            execution_id = %execution_id,
            new_execution_id = %new_execution_id,
            "Appending WorkflowExecutionRestartedFresh event"
        );

        let entry_payload = serde_json::json!({
            "new_execution_id": new_execution_id,
            "restart_payload": payload,
        });

        self.append_entry(
            workflow_id,
            execution_id,
            "WorkflowExecutionRestartedFresh",
            entry_payload,
        )
        .await
    }

    // ========================================================================
    // EXTERNAL EVENT ENTRIES
    // ========================================================================

    /// Append EventReceived entry to a workflow's journal
    ///
    /// Records that an external event/signal was delivered to this workflow.
    /// The SDK will encounter this entry during replay and deliver it to the
    /// matching `wait_for_event` handler.
    ///
    /// # Arguments
    ///
    /// * `workflow_id` - The target workflow identifier (internal UUID as string)
    /// * `execution_id` - The target execution identifier
    /// * `event_name` - Name of the event (matches SDK `wait_for_event("name")`)
    /// * `payload` - Raw event payload bytes
    /// * `identity` - Who/what sent the event (request_id or sender identity)
    pub async fn append_event_received(
        &self,
        workflow_id: &str,
        execution_id: &str,
        event_name: &str,
        payload: &[u8],
        identity: &str,
    ) -> OrcherResult<i64> {
        info!(
            workflow_id = %workflow_id,
            execution_id = %execution_id,
            event_name = %event_name,
            payload_size = payload.len(),
            "Appending EventReceived entry"
        );

        let payload = serde_json::json!({
            "event_name": event_name,
            "payload": base64::engine::general_purpose::STANDARD.encode(payload),
            "identity": identity,
        });

        self.append_entry(workflow_id, execution_id, "EventReceived", payload)
            .await
    }

    /// [`append_event_received`](Self::append_event_received), unless the
    /// execution already holds an EventReceived from `identity`. Returns
    /// whether it was appended.
    ///
    /// For an event delivered on behalf of a request that may be repeated —
    /// the event a SendEventWithStartWorkflow seeds into the run it starts —
    /// keyed by the request's id, so a repeat of the request does not deliver
    /// it twice. The check and the append are one step under the execution's
    /// journal lock.
    pub async fn append_event_received_once(
        &self,
        workflow_id: &str,
        execution_id: &str,
        event_name: &str,
        payload: &[u8],
        identity: &str,
    ) -> OrcherResult<bool> {
        let entry = JournalEntry::new(
            workflow_id.to_string(),
            execution_id.to_string(),
            0,
            "EventReceived".to_string(),
            serde_json::json!({
                "event_name": event_name,
                "payload": base64::engine::general_purpose::STANDARD.encode(payload),
                "identity": identity,
            }),
        );
        let appended = self
            .repository
            .append_next_once(entry, &["EventReceived"], "identity", identity)
            .await?;
        Ok(appended.is_some())
    }

    // ========================================================================
    // CHILD WORKFLOW EVENTS
    // ========================================================================

    /// Append ChildWorkflowExecutionInitiated event
    ///
    /// Called when a parent workflow starts a child workflow.
    ///
    /// # Arguments
    ///
    /// * `workflow_id` - The parent workflow identifier
    /// * `execution_id` - The parent execution identifier
    /// * `child_workflow_id` - The child workflow identifier
    /// * `child_execution_id` - The child's run id
    /// * `child_workflow_type` - The child workflow type
    /// * `task_queue` - The task queue for child execution
    /// * `input` - The input data for child workflow
    pub async fn append_child_workflow_initiated(
        &self,
        workflow_id: &str,
        execution_id: &str,
        child_workflow_id: &str,
        child_execution_id: &str,
        child_workflow_type: &str,
        task_queue: &str,
        input: serde_json::Value,
    ) -> OrcherResult<i64> {
        info!(
            workflow_id = %workflow_id,
            execution_id = %execution_id,
            child_workflow_id = %child_workflow_id,
            "Appending ChildWorkflowExecutionInitiated event"
        );

        let payload = serde_json::json!({
            "child_workflow_id": child_workflow_id,
            "child_execution_id": child_execution_id,
            "child_workflow_type": child_workflow_type,
            "task_queue": task_queue,
            "input": input,
        });

        self.append_entry(
            workflow_id,
            execution_id,
            "ChildWorkflowExecutionInitiated",
            payload,
        )
        .await
    }

    /// Append ChildWorkflowExecutionCompleted event
    ///
    /// Called when a child workflow completes successfully.
    ///
    /// # Arguments
    ///
    /// * `workflow_id` - The parent workflow identifier
    /// * `execution_id` - The parent execution identifier
    /// * `child_workflow_id` - The child workflow identifier
    /// * `child_execution_id` - The child execution identifier
    /// * `initiated_event_id` - The event ID from the initiated event
    /// * `started_event_id` - The event ID from the started event
    /// * `result` - The child workflow result
    pub async fn append_child_workflow_completed(
        &self,
        workflow_id: &str,
        execution_id: &str,
        child_workflow_id: &str,
        child_execution_id: &str,
        initiated_event_id: i64,
        started_event_id: i64,
        result: serde_json::Value,
    ) -> OrcherResult<i64> {
        info!(
            workflow_id = %workflow_id,
            execution_id = %execution_id,
            child_workflow_id = %child_workflow_id,
            child_execution_id = %child_execution_id,
            "Appending ChildWorkflowExecutionCompleted event"
        );

        let payload = serde_json::json!({
            "child_workflow_id": child_workflow_id,
            "child_execution_id": child_execution_id,
            "initiated_event_id": initiated_event_id,
            "started_event_id": started_event_id,
            "result": result,
        });

        self.append_entry(
            workflow_id,
            execution_id,
            "ChildWorkflowExecutionCompleted",
            payload,
        )
        .await
    }

    /// Append ChildWorkflowExecutionFailed event
    ///
    /// Called when a child workflow fails.
    ///
    /// # Arguments
    ///
    /// * `workflow_id` - The parent workflow identifier
    /// * `execution_id` - The parent execution identifier
    /// * `child_workflow_id` - The child workflow identifier
    /// * `child_execution_id` - The child execution identifier
    /// * `initiated_event_id` - The event ID from the initiated event
    /// * `started_event_id` - The event ID from the started event
    /// * `failure_reason` - Reason for failure
    /// * `failure_details` - Optional detailed failure information
    pub async fn append_child_workflow_failed(
        &self,
        workflow_id: &str,
        execution_id: &str,
        child_workflow_id: &str,
        child_execution_id: &str,
        initiated_event_id: i64,
        started_event_id: i64,
        failure_reason: String,
        failure_details: Option<serde_json::Value>,
    ) -> OrcherResult<i64> {
        info!(
            workflow_id = %workflow_id,
            execution_id = %execution_id,
            child_workflow_id = %child_workflow_id,
            child_execution_id = %child_execution_id,
            failure_reason = %failure_reason,
            "Appending ChildWorkflowExecutionFailed event"
        );

        let payload = serde_json::json!({
            "child_workflow_id": child_workflow_id,
            "child_execution_id": child_execution_id,
            "initiated_event_id": initiated_event_id,
            "started_event_id": started_event_id,
            "failure_reason": failure_reason,
            "failure_details": failure_details,
        });

        self.append_entry(
            workflow_id,
            execution_id,
            "ChildWorkflowExecutionFailed",
            payload,
        )
        .await
    }

    /// Append ChildWorkflowExecutionTimedOut event
    ///
    /// # Arguments
    ///
    /// * `workflow_id` - The parent workflow identifier
    /// * `execution_id` - The parent execution identifier
    /// * `child_workflow_id` - The child workflow identifier
    /// * `child_execution_id` - The child execution identifier
    /// * `initiated_event_id` - The event ID from the initiated event
    /// * `started_event_id` - The event ID from the started event
    pub async fn append_child_workflow_timed_out(
        &self,
        workflow_id: &str,
        execution_id: &str,
        child_workflow_id: &str,
        child_execution_id: &str,
        initiated_event_id: i64,
        started_event_id: i64,
    ) -> OrcherResult<i64> {
        info!(
            workflow_id = %workflow_id,
            execution_id = %execution_id,
            child_workflow_id = %child_workflow_id,
            "Appending ChildWorkflowExecutionTimedOut event"
        );

        let payload = serde_json::json!({
            "child_workflow_id": child_workflow_id,
            "child_execution_id": child_execution_id,
            "initiated_event_id": initiated_event_id,
            "started_event_id": started_event_id,
        });

        self.append_entry(
            workflow_id,
            execution_id,
            "ChildWorkflowExecutionTimedOut",
            payload,
        )
        .await
    }

    /// Append ChildWorkflowExecutionCanceled event
    ///
    /// # Arguments
    ///
    /// * `workflow_id` - The parent workflow identifier
    /// * `execution_id` - The parent execution identifier
    /// * `child_workflow_id` - The child workflow identifier
    /// * `child_execution_id` - The child execution identifier
    /// * `initiated_event_id` - The event ID from the initiated event
    /// * `started_event_id` - Optional event ID from the started event (if child started)
    pub async fn append_child_workflow_canceled(
        &self,
        workflow_id: &str,
        execution_id: &str,
        child_workflow_id: &str,
        child_execution_id: &str,
        initiated_event_id: i64,
        started_event_id: Option<i64>,
    ) -> OrcherResult<i64> {
        info!(
            workflow_id = %workflow_id,
            execution_id = %execution_id,
            child_workflow_id = %child_workflow_id,
            "Appending ChildWorkflowExecutionCanceled event"
        );

        let payload = serde_json::json!({
            "child_workflow_id": child_workflow_id,
            "child_execution_id": child_execution_id,
            "initiated_event_id": initiated_event_id,
            "started_event_id": started_event_id,
        });

        self.append_entry(
            workflow_id,
            execution_id,
            "ChildWorkflowExecutionCanceled",
            payload,
        )
        .await
    }

    /// Append ChildWorkflowExecutionTerminated event
    ///
    /// # Arguments
    ///
    /// * `workflow_id` - The parent workflow identifier
    /// * `execution_id` - The parent execution identifier
    /// * `child_workflow_id` - The child workflow identifier
    /// * `child_execution_id` - The child execution identifier
    /// * `initiated_event_id` - The event ID from the initiated event
    /// * `started_event_id` - The event ID from the started event
    /// * `reason` - Reason for termination
    pub async fn append_child_workflow_terminated(
        &self,
        workflow_id: &str,
        execution_id: &str,
        child_workflow_id: &str,
        child_execution_id: &str,
        initiated_event_id: i64,
        started_event_id: i64,
        reason: String,
    ) -> OrcherResult<i64> {
        info!(
            workflow_id = %workflow_id,
            execution_id = %execution_id,
            child_workflow_id = %child_workflow_id,
            reason = %reason,
            "Appending ChildWorkflowExecutionTerminated event"
        );

        let payload = serde_json::json!({
            "child_workflow_id": child_workflow_id,
            "child_execution_id": child_execution_id,
            "initiated_event_id": initiated_event_id,
            "started_event_id": started_event_id,
            "reason": reason,
        });

        self.append_entry(
            workflow_id,
            execution_id,
            "ChildWorkflowExecutionTerminated",
            payload,
        )
        .await
    }

    // ========================================================================
    // TASK EVENTS
    // ========================================================================

    /// Append TaskScheduled event
    ///
    /// # Arguments
    ///
    /// * `workflow_id` - The workflow identifier
    /// * `execution_id` - The execution identifier
    /// * `task_id` - The task identifier
    /// * `task_type` - The task type
    /// * `input` - The task input
    pub async fn append_task_scheduled(
        &self,
        workflow_id: &str,
        execution_id: &str,
        task_id: &str,
        task_type: &str,
        input: serde_json::Value,
    ) -> OrcherResult<i64> {
        debug!(
            workflow_id = %workflow_id,
            execution_id = %execution_id,
            task_id = %task_id,
            "Appending TaskScheduled event"
        );

        let payload = serde_json::json!({
            "task_id": task_id,
            "task_type": task_type,
            "input": input,
        });

        self.append_entry(workflow_id, execution_id, "TaskScheduled", payload)
            .await
    }

    /// Append TaskStarted event
    ///
    /// # Arguments
    ///
    /// * `workflow_id` - The workflow identifier
    /// * `execution_id` - The execution identifier
    /// * `task_id` - The task identifier
    /// * `scheduled_event_id` - The event ID from the TaskScheduled event
    pub async fn append_task_started(
        &self,
        workflow_id: &str,
        execution_id: &str,
        task_id: i64,
        scheduled_event_id: i64,
    ) -> OrcherResult<i64> {
        debug!(
            workflow_id = %workflow_id,
            execution_id = %execution_id,
            task_id = task_id,
            "Appending TaskStarted event"
        );

        let payload = serde_json::json!({
            "task_id": task_id,
            "scheduled_event_id": scheduled_event_id,
        });

        self.append_entry_with_task(workflow_id, execution_id, "TaskStarted", payload, task_id)
            .await
    }

    /// Append TaskCompleted event
    ///
    /// # Arguments
    ///
    /// * `workflow_id` - The workflow identifier
    /// * `execution_id` - The execution identifier
    /// * `task_id` - The task identifier
    /// * `scheduled_event_id` - The event ID from the TaskScheduled event
    /// * `started_event_id` - The event ID from the TaskStarted event
    /// * `result` - The task result
    pub async fn append_task_completed(
        &self,
        workflow_id: &str,
        execution_id: &str,
        task_id: i64,
        scheduled_event_id: i64,
        started_event_id: i64,
        result: serde_json::Value,
    ) -> OrcherResult<i64> {
        debug!(
            workflow_id = %workflow_id,
            execution_id = %execution_id,
            task_id = task_id,
            "Appending TaskCompleted event"
        );

        let payload = serde_json::json!({
            "task_id": task_id,
            "scheduled_event_id": scheduled_event_id,
            "started_event_id": started_event_id,
            "result": result,
        });

        self.append_entry_with_task(workflow_id, execution_id, "TaskCompleted", payload, task_id)
            .await
    }

    /// Append TaskFailed event
    ///
    /// # Arguments
    ///
    /// * `workflow_id` - The workflow identifier
    /// * `execution_id` - The execution identifier
    /// * `task_id` - The task identifier
    /// * `scheduled_event_id` - The event ID from the TaskScheduled event
    /// * `started_event_id` - The event ID from the TaskStarted event
    /// * `failure_reason` - Reason for failure
    /// * `failure_details` - Optional detailed failure information
    pub async fn append_task_failed(
        &self,
        workflow_id: &str,
        execution_id: &str,
        task_id: i64,
        scheduled_event_id: i64,
        started_event_id: i64,
        failure_reason: String,
        failure_details: Option<serde_json::Value>,
    ) -> OrcherResult<i64> {
        debug!(
            workflow_id = %workflow_id,
            execution_id = %execution_id,
            task_id = task_id,
            failure_reason = %failure_reason,
            "Appending TaskFailed event"
        );

        let payload = serde_json::json!({
            "task_id": task_id,
            "scheduled_event_id": scheduled_event_id,
            "started_event_id": started_event_id,
            "failure_reason": failure_reason,
            "failure_details": failure_details,
        });

        self.append_entry_with_task(workflow_id, execution_id, "TaskFailed", payload, task_id)
            .await
    }

    // ========================================================================
    // TIMER EVENTS
    // ========================================================================

    /// Append TimerStarted event
    ///
    /// # Arguments
    ///
    /// * `workflow_id` - The workflow identifier
    /// * `execution_id` - The execution identifier
    /// * `timer_id` - The timer identifier
    /// * `duration_ms` - The timer duration in milliseconds
    pub async fn append_timer_started(
        &self,
        workflow_id: &str,
        execution_id: &str,
        timer_id: &str,
        duration_ms: i64,
    ) -> OrcherResult<i64> {
        debug!(
            workflow_id = %workflow_id,
            execution_id = %execution_id,
            timer_id = %timer_id,
            "Appending TimerStarted event"
        );

        let payload = serde_json::json!({
            "timer_id": timer_id,
            "duration_ms": duration_ms,
        });

        self.append_entry(workflow_id, execution_id, "TimerStarted", payload)
            .await
    }

    /// Append TimerStarted for `timer_id` unless this execution's journal
    /// already has one. Returns whether it was appended.
    pub async fn append_timer_started_once(
        &self,
        workflow_id: &str,
        execution_id: &str,
        timer_id: &str,
        duration_ms: i64,
    ) -> OrcherResult<bool> {
        let entry = JournalEntry::new(
            workflow_id.to_string(),
            execution_id.to_string(),
            0,
            "TimerStarted".to_string(),
            serde_json::json!({
                "timer_id": timer_id,
                "duration_ms": duration_ms,
            }),
        );
        Ok(self
            .repository
            .append_next_once(entry, &["TimerStarted"], "timer_id", timer_id)
            .await?
            .is_some())
    }

    /// Append TimerFired event
    ///
    /// # Arguments
    ///
    /// * `workflow_id` - The workflow identifier
    /// * `execution_id` - The execution identifier
    /// * `timer_id` - The timer identifier
    /// * `started_event_id` - The event ID from the TimerStarted event
    pub async fn append_timer_fired(
        &self,
        workflow_id: &str,
        execution_id: &str,
        timer_id: &str,
        started_event_id: i64,
    ) -> OrcherResult<i64> {
        debug!(
            workflow_id = %workflow_id,
            execution_id = %execution_id,
            timer_id = %timer_id,
            "Appending TimerFired event"
        );

        let payload = serde_json::json!({
            "timer_id": timer_id,
            "started_event_id": started_event_id,
        });

        self.append_entry(workflow_id, execution_id, "TimerFired", payload)
            .await
    }

    /// Append TimerCanceled event
    ///
    /// # Arguments
    ///
    /// * `workflow_id` - The workflow identifier
    /// * `execution_id` - The execution identifier
    /// * `timer_id` - The timer identifier
    /// * `started_event_id` - The event ID from the TimerStarted event
    pub async fn append_timer_canceled(
        &self,
        workflow_id: &str,
        execution_id: &str,
        timer_id: &str,
        started_event_id: i64,
    ) -> OrcherResult<i64> {
        debug!(
            workflow_id = %workflow_id,
            execution_id = %execution_id,
            timer_id = %timer_id,
            "Appending TimerCanceled event"
        );

        let payload = serde_json::json!({
            "timer_id": timer_id,
            "started_event_id": started_event_id,
        });

        self.append_entry(workflow_id, execution_id, "TimerCanceled", payload)
            .await
    }

    // ========================================================================
    // QUERY METHODS
    // ========================================================================

    /// Get the complete journal for a workflow execution
    ///
    /// # Arguments
    ///
    /// * `workflow_id` - The workflow identifier
    /// * `execution_id` - The execution identifier
    pub async fn get_journal(
        &self,
        workflow_id: &str,
        execution_id: &str,
    ) -> OrcherResult<Vec<JournalEntry>> {
        self.repository.get_journal(workflow_id, execution_id).await
    }

    /// Get a specific journal entry
    ///
    /// # Arguments
    ///
    /// * `workflow_id` - The workflow identifier
    /// * `execution_id` - The execution identifier
    /// * `event_id` - The event identifier
    pub async fn get_entry(
        &self,
        workflow_id: &str,
        execution_id: &str,
        event_id: i64,
    ) -> OrcherResult<Option<JournalEntry>> {
        self.repository
            .get_entry(workflow_id, execution_id, event_id)
            .await
    }
}

#[cfg(test)]
mod tests {

    #[test]
    fn test_journal_service_creation() {
        // Compile-time test to ensure JournalService can be constructed
        fn _assert_service_constructible<T>(_service: T) {}

        // This would compile if we had a mock repository
        // let repo = Arc::new(mock_journal_repository());
        // let service = JournalService::new(repo);
        // _assert_service_constructible(service);
    }
}
