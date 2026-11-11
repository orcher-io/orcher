//! Type definitions for the execution polling system.

use crate::types::time::Timestamptz;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;

/// Request to poll for workflow execution work.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PollExecutionRequest {
    pub task_queue: String,
    pub namespace: String,
    pub identity: String,
    pub poll_timeout: Duration,
    /// Code release this worker is running, if it declares one.
    ///
    /// Opaque — compared for equality, never parsed or ordered — so the same
    /// field can identify a deployed binary now and an addressable endpoint
    /// later. `None` means the worker declares nothing, which is the default.
    pub version_id: Option<String>,
    /// The organisation the poll was made for, if it named one. Used to tell
    /// whether the polling worker has said it is shutting down.
    #[serde(default)]
    pub organization_id: Option<uuid::Uuid>,
    /// The process the poll came from, if the worker said. A shutdown that
    /// names a process refuses only its polls, so a later process reusing
    /// the identity is served.
    #[serde(default)]
    pub worker_instance_id: Option<String>,
    /// The step-id schemes the worker supports, as `orcher.v1.StepIdScheme`
    /// numbers them. Empty for a worker that reports none, which is given
    /// only runs no scheme is bound to.
    #[serde(default)]
    pub step_ids: Vec<i16>,
    /// The worker process keeps each run's journal between activations, so
    /// it may be sent only the entries after those it was sent before.
    #[serde(default)]
    pub history_cached: bool,
}

impl PollExecutionRequest {
    pub fn new(
        task_queue: impl Into<String>,
        namespace: impl Into<String>,
        identity: impl Into<String>,
        poll_timeout: Duration,
    ) -> Self {
        Self {
            task_queue: task_queue.into(),
            namespace: namespace.into(),
            identity: identity.into(),
            poll_timeout,
            version_id: None,
            organization_id: None,
            worker_instance_id: None,
            step_ids: Vec::new(),
            history_cached: false,
        }
    }

    /// Whether the worker process keeps run journals between activations.
    pub fn with_history_cached(mut self, history_cached: bool) -> Self {
        self.history_cached = history_cached;
        self
    }

    /// The process the poll came from; empty means it did not say.
    pub fn with_worker_instance_id(mut self, worker_instance_id: Option<String>) -> Self {
        self.worker_instance_id = worker_instance_id.filter(|id| !id.is_empty());
        self
    }

    /// The step-id schemes the worker supports, as the poll numbers them
    /// (`orcher.v1.StepIdScheme`). The unspecified scheme, and any number no
    /// scheme could have, name none and are left out.
    pub fn with_step_ids(mut self, step_ids: &[i32]) -> Self {
        let mut step_ids: Vec<i16> = step_ids
            .iter()
            .filter(|&&id| id > 0)
            .filter_map(|&id| i16::try_from(id).ok())
            .collect();
        step_ids.sort_unstable();
        step_ids.dedup();
        self.step_ids = step_ids;
        self
    }

    /// The organisation the poll was made for.
    pub fn with_organization_id(mut self, organization_id: Option<uuid::Uuid>) -> Self {
        self.organization_id = organization_id;
        self
    }

    /// Declare the code release this worker is running.
    pub fn with_version_id(mut self, version_id: Option<String>) -> Self {
        self.version_id = version_id.filter(|v| !v.is_empty());
        self
    }

    /// Build a `namespace:task_queue` key for notification routing.
    pub fn queue_key(&self) -> String {
        format!("{}:{}", self.namespace, self.task_queue)
    }
}

/// Response containing workflow execution work.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PollExecutionResponse {
    pub workflow_id: String,
    pub execution_id: String,
    pub workflow_type: String,
    pub task_queue: String,
    pub namespace: String,
    pub input: serde_json::Value,
    pub journal: Vec<JournalEntry>,
    pub started_event_id: i64,
    pub previous_started_event_id: Option<i64>,
    pub attempt: i32,
    pub started_at: Timestamptz,
    pub claimed_at: Timestamptz,
    pub claim_timeout_at: Timestamptz,
    pub claimed_by: String,
    /// Redis Stream entry ID for acknowledgment.
    /// Used with Redis Streams consumer groups to ACK after processing.
    pub stream_entry_id: Option<String>,
    /// The execution_id of the prior run when this execution was created by RestartFresh.
    /// Empty for first-time executions.
    pub parent_execution_id: Option<String>,
    /// The number the claim gave this activation. Handed to the worker inside
    /// its activation token, so its completion can be matched to it.
    #[serde(default)]
    pub activation_seq: i64,
    /// The step-id scheme the run is bound to, as `orcher.v1.StepIdScheme`
    /// numbers it; `None` for a run claimed by a worker reporting none.
    #[serde(default)]
    pub step_id_scheme: Option<i16>,
    /// The run's history lineage, bound with the scheme.
    #[serde(default)]
    pub history_lineage: String,
    /// Whether the run's journal holds its step results as `StepCompleted`
    /// entries; when not, they are rebuilt from its execution log.
    #[serde(default)]
    pub step_results_journaled: bool,
    /// Which part of the run's journal `journal` is: 0 the whole of it, N
    /// only the entries after entry N, which the polling worker process was
    /// sent before.
    #[serde(default)]
    pub history_after_entry_id: i64,
}

/// Single event in the execution journal.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JournalEntry {
    pub event_id: i64,
    pub timestamp: Timestamptz,
    pub entry_type: String,
    pub payload: Option<serde_json::Value>,
}

impl JournalEntry {
    pub fn new(event_id: i64, entry_type: impl Into<String>, payload: serde_json::Value) -> Self {
        Self {
            event_id,
            timestamp: Timestamptz::now(),
            entry_type: entry_type.into(),
            payload: Some(payload),
        }
    }

    pub fn with_payload(
        event_id: i64,
        entry_type: impl Into<String>,
        payload: serde_json::Value,
    ) -> Self {
        Self {
            event_id,
            timestamp: Timestamptz::now(),
            entry_type: entry_type.into(),
            payload: Some(payload),
        }
    }
}

/// Request to poll for task (activity) work.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PollTaskRequest {
    pub task_queue: String,
    pub namespace: String,
    pub identity: String,
    pub poll_timeout: Duration,
}

impl PollTaskRequest {
    pub fn new(
        task_queue: impl Into<String>,
        namespace: impl Into<String>,
        identity: impl Into<String>,
        poll_timeout: Duration,
    ) -> Self {
        Self {
            task_queue: task_queue.into(),
            namespace: namespace.into(),
            identity: identity.into(),
            poll_timeout,
        }
    }

    /// Build a `namespace:task_queue` key for notification routing.
    pub fn queue_key(&self) -> String {
        format!("{}:{}", self.namespace, self.task_queue)
    }
}

/// Response containing task execution work.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PollTaskResponse {
    pub task_id: String,
    pub task_type: String,
    pub workflow_id: String,
    pub execution_id: String,
    pub input: serde_json::Value,
    pub attempt: i32,
    pub start_to_close_timeout: Option<Duration>,
    pub heartbeat_timeout: Option<Duration>,
    pub task_queue: String,
    pub namespace: String,
}

/// Tracks which service claimed an execution and when it expires.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClaimInfo {
    pub execution_id: String,
    pub claimed_by: String,
    pub claimed_at: Timestamptz,
    pub claim_timeout_at: Timestamptz,
}

impl ClaimInfo {
    pub fn is_expired(&self) -> bool {
        Timestamptz::now() > self.claim_timeout_at
    }

    pub fn remaining_duration(&self) -> Duration {
        let remaining = self.claim_timeout_at - Timestamptz::now();
        if remaining.as_secs() <= 0 {
            Duration::from_secs(0)
        } else {
            Duration::from_secs(remaining.as_secs() as u64)
        }
    }
}

/// Statistics about a task queue.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueStats {
    pub task_queue: String,
    pub namespace: String,
    pub available_count: i64,
    pub claimed_count: i64,
    pub running_count: i64,
    pub timestamp: Timestamptz,
}

impl QueueStats {
    pub fn new(task_queue: String, namespace: String) -> Self {
        Self {
            task_queue,
            namespace,
            available_count: 0,
            claimed_count: 0,
            running_count: 0,
            timestamp: Timestamptz::now(),
        }
    }

    pub fn total_count(&self) -> i64 {
        self.available_count + self.claimed_count + self.running_count
    }

    pub fn has_available_work(&self) -> bool {
        self.available_count > 0
    }
}

/// Notification for task queue state changes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueNotification {
    pub task_queue: String,
    pub namespace: String,
    pub notification_type: NotificationType,
    pub timestamp: Timestamptz,
}

/// Type of queue notification.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NotificationType {
    /// New work is available.
    WorkAvailable,

    /// Execution completed.
    ExecutionCompleted,

    /// Execution failed.
    ExecutionFailed,

    /// Claim expired and work was reclaimed.
    ClaimExpired,
}

// ============================================================================
// Zero-Copy Types
// ============================================================================

/// Arc-wrapped poll response for zero-copy dispatch across multiple waiters.
///
/// Avoids cloning the full response when retrying dispatch (e.g., if the
/// first waiter's channel is closed) or sharing across threads.
#[derive(Debug, Clone)]
pub struct SharedPollResponse {
    inner: Arc<PollExecutionResponse>,
}

impl SharedPollResponse {
    pub fn new(response: PollExecutionResponse) -> Self {
        Self {
            inner: Arc::new(response),
        }
    }

    /// Create from an existing `Arc` (no additional allocation).
    pub fn from_arc(inner: Arc<PollExecutionResponse>) -> Self {
        Self { inner }
    }

    #[inline]
    pub fn ref_count(&self) -> usize {
        Arc::strong_count(&self.inner)
    }

    /// Unwrap the inner response if this is the sole reference.
    pub fn try_unwrap(self) -> Result<PollExecutionResponse, Self> {
        match Arc::try_unwrap(self.inner) {
            Ok(response) => Ok(response),
            Err(arc) => Err(Self { inner: arc }),
        }
    }

    pub fn to_owned(&self) -> PollExecutionResponse {
        (*self.inner).clone()
    }

    #[inline]
    pub fn workflow_id(&self) -> &str {
        &self.inner.workflow_id
    }

    #[inline]
    pub fn execution_id(&self) -> &str {
        &self.inner.execution_id
    }

    #[inline]
    pub fn task_queue(&self) -> &str {
        &self.inner.task_queue
    }

    #[inline]
    pub fn namespace(&self) -> &str {
        &self.inner.namespace
    }
}

impl From<PollExecutionResponse> for SharedPollResponse {
    fn from(response: PollExecutionResponse) -> Self {
        Self::new(response)
    }
}

impl AsRef<PollExecutionResponse> for SharedPollResponse {
    fn as_ref(&self) -> &PollExecutionResponse {
        &self.inner
    }
}

impl std::ops::Deref for SharedPollResponse {
    type Target = PollExecutionResponse;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_poll_request_queue_key() {
        let request = PollExecutionRequest::new(
            "my-queue",
            "production",
            "service-1",
            Duration::from_secs(60),
        );

        assert_eq!(request.queue_key(), "production:my-queue");
    }

    #[test]
    fn test_claim_info_expiry() {
        let claim = ClaimInfo {
            execution_id: "exec-123".to_string(),
            claimed_by: "service-1".to_string(),
            claimed_at: Timestamptz::now() - jiff::SignedDuration::from_secs(10),
            claim_timeout_at: Timestamptz::now() - jiff::SignedDuration::from_secs(5),
        };

        assert!(claim.is_expired());
    }

    #[test]
    fn test_claim_info_not_expired() {
        let claim = ClaimInfo {
            execution_id: "exec-123".to_string(),
            claimed_by: "service-1".to_string(),
            claimed_at: Timestamptz::now(),
            claim_timeout_at: Timestamptz::now() + jiff::SignedDuration::from_secs(300),
        };

        assert!(!claim.is_expired());
    }

    #[test]
    fn test_queue_stats_total_count() {
        let mut stats = QueueStats::new("test-queue".to_string(), "test".to_string());
        stats.available_count = 5;
        stats.claimed_count = 3;
        stats.running_count = 2;

        assert_eq!(stats.total_count(), 10);
    }

    #[test]
    fn test_queue_stats_has_work() {
        let mut stats = QueueStats::new("test-queue".to_string(), "test".to_string());

        assert!(!stats.has_available_work());

        stats.available_count = 1;
        assert!(stats.has_available_work());
    }

    #[test]
    fn test_journal_entry_creation() {
        let entry = JournalEntry::new(
            1,
            "WorkflowExecutionStarted",
            serde_json::json!({"workflow_id": "wf-123"}),
        );

        assert_eq!(entry.event_id, 1);
        assert_eq!(entry.entry_type, "WorkflowExecutionStarted");
        assert!(entry.payload.is_some());
    }

    #[test]
    fn test_journal_entry_with_payload() {
        let entry = JournalEntry::with_payload(
            1,
            "WorkflowExecutionStarted",
            serde_json::json!({"workflow_id": "wf-123"}),
        );

        assert!(entry.payload.is_some());
    }
}
