//! Proto-to-domain type conversions for dispatch.

use orcher_common::OrcherResult;
use orcher_storage::{
    repositories::domains::journal::JournalRepository, schema::types::execution::WorkflowExecution,
};
use std::sync::Arc;
use tracing::{debug, info};

use crate::engine::state::high_perf_cache::HighPerfCache;
use orcher_common::types::dispatch::polling::{JournalEntry, PollExecutionResponse};

/// Converter for transforming DB types to polling response types
///
/// Loads journal entries from the database and converts WorkflowExecution
/// into a format suitable for service consumption. Optionally uses incremental
/// journal loading via HighPerfCache to reduce database reads.
pub struct Converter {
    /// Repository for loading journal entries
    journal_repo: Arc<JournalRepository>,
    /// Optional high-performance cache for incremental journal loading
    cache: Option<Arc<HighPerfCache>>,
}

impl Converter {
    /// Create a new converter
    ///
    /// # Arguments
    ///
    /// * `journal_repo` - Repository for loading journal entries
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let converter = Converter::new(journal_repo);
    /// ```
    pub fn new(journal_repo: Arc<JournalRepository>) -> Self {
        debug!("Creating converter without cache");
        Self {
            journal_repo,
            cache: None,
        }
    }

    /// Create a new converter with cache support for incremental loading
    ///
    /// When a cache is provided, the converter will use incremental journal
    /// loading - only fetching new entries since the last cached event_id.
    /// This significantly reduces database load for frequently-polled executions.
    ///
    /// # Arguments
    ///
    /// * `journal_repo` - Repository for loading journal entries
    /// * `cache` - High-performance cache for journal entries
    pub fn with_cache(journal_repo: Arc<JournalRepository>, cache: Arc<HighPerfCache>) -> Self {
        debug!("Creating converter with cache for incremental loading");
        Self {
            journal_repo,
            cache: Some(cache),
        }
    }

    /// Convert WorkflowExecution to PollExecutionResponse
    ///
    /// Loads the execution journal from the database and constructs a complete
    /// response that services can use to execute workflow code.
    ///
    /// # Arguments
    ///
    /// * `execution` - The claimed workflow execution
    ///
    /// # Returns
    ///
    /// A complete poll response with execution context and journal
    ///
    /// # Errors
    ///
    /// Returns an error if journal entries cannot be loaded from the database
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let execution = claimer.claim_execution(...).await?;
    /// let response = converter.to_poll_response(&execution).await?;
    ///
    /// // Service can now execute workflow with journal
    /// ```
    pub async fn to_poll_response(
        &self,
        execution: &WorkflowExecution,
    ) -> OrcherResult<PollExecutionResponse> {
        self.to_poll_response_after(execution, 0).await
    }

    /// [`to_poll_response`](Self::to_poll_response), carrying only the
    /// journal entries after entry `after` when it is above 0 — for a worker
    /// process holding the run's entries up to it — and read as only those.
    pub async fn to_poll_response_after(
        &self,
        execution: &WorkflowExecution,
        after: i64,
    ) -> OrcherResult<PollExecutionResponse> {
        debug!(
            execution_id = %execution.execution_id,
            workflow_id = %execution.id,
            "Converting execution to poll response"
        );

        // Load journal entries - use incremental loading if cache is available
        let workflow_id_str = execution.id.as_uuid().to_string();
        let execution_id_str = &execution.execution_id;

        let db_journal = if after > 0 {
            self.journal_repo
                .get_entries_after(&workflow_id_str, execution_id_str, after)
                .await?
        } else if let Some(cache) = &self.cache {
            // Use incremental loading via cache
            let journal_repo = self.journal_repo.clone();
            let workflow_id = workflow_id_str.clone();
            let exec_id = execution_id_str.clone();

            // Full loader - loads all entries when cache miss
            let full_loader = {
                let journal_repo = journal_repo.clone();
                let workflow_id = workflow_id.clone();
                let exec_id = exec_id.clone();
                move || {
                    let journal_repo = journal_repo.clone();
                    let workflow_id = workflow_id.clone();
                    let exec_id = exec_id.clone();
                    async move { journal_repo.get_journal(&workflow_id, &exec_id).await }
                }
            };

            // Incremental loader - loads only entries after last_event_id
            let incremental_loader = {
                let journal_repo = journal_repo.clone();
                let workflow_id = workflow_id.clone();
                let exec_id = exec_id.clone();
                move |after_event_id: i64| {
                    let journal_repo = journal_repo.clone();
                    let workflow_id = workflow_id.clone();
                    let exec_id = exec_id.clone();
                    async move {
                        journal_repo
                            .get_entries_after(&workflow_id, &exec_id, after_event_id)
                            .await
                    }
                }
            };

            let entries = cache
                .get_or_load_journal_incremental(execution_id_str, full_loader, incremental_loader)
                .await?;

            info!(
                execution_id = %execution.execution_id,
                journal_entries = entries.len(),
                "Loaded journal entries via incremental cache"
            );

            entries
        } else {
            // Fallback: load all entries directly from database
            let entries = self
                .journal_repo
                .get_journal(&workflow_id_str, execution_id_str)
                .await?;

            debug!(
                execution_id = %execution.execution_id,
                journal_entries = entries.len(),
                "Loaded journal entries (no cache)"
            );

            entries
        };

        // Convert journal entries to polling format
        let journal: Vec<JournalEntry> = db_journal
            .into_iter()
            .map(|entry| JournalEntry {
                event_id: entry.event_id,
                timestamp: entry.timestamp,
                entry_type: entry.event_type,
                payload: Some(entry.payload),
            })
            .collect();

        // Determine started event ID
        // For new executions, this is typically 1 (WorkflowExecutionStarted)
        let started_event_id = journal
            .iter()
            .find(|e| e.entry_type == "WorkflowExecutionStarted")
            .map(|e| e.event_id)
            .unwrap_or(1);

        // For resumed executions, previous_started_event_id would be set
        // For now, we'll use None (first execution)
        let previous_started_event_id = None;

        // Build response
        let response = PollExecutionResponse {
            workflow_id: execution.id.as_uuid().to_string(),
            execution_id: execution.execution_id.clone(), // Use the external-facing execution_id (e.g., "exec_uuid")
            workflow_type: execution.workflow_type.clone(),
            task_queue: execution.task_queue.clone(),
            namespace: execution.namespace.clone(),
            input: execution.parameters.clone(),
            journal,
            started_event_id,
            previous_started_event_id,
            attempt: workflow_attempt(execution.retry_count),
            started_at: execution.started_at.unwrap_or(execution.submitted_at),
            claimed_at: execution
                .claimed_at
                .expect("Claimed execution must have claimed_at"),
            claim_timeout_at: execution
                .claim_timeout_at
                .expect("Claimed execution must have claim_timeout_at"),
            claimed_by: execution
                .claimed_by
                .clone()
                .expect("Claimed execution must have claimed_by"),
            stream_entry_id: None,
            parent_execution_id: execution.parent_execution_id.clone(),
            activation_seq: execution.activation_seq,
            step_id_scheme: execution.step_id_scheme,
            history_lineage: execution.lineage(),
            step_results_journaled: execution.step_results_journaled,
            history_after_entry_id: after,
        };

        debug!(
            execution_id = %response.execution_id,
            journal_entries = response.journal.len(),
            attempt = response.attempt,
            "Successfully converted to poll response"
        );

        Ok(response)
    }

    /// Convert WorkflowExecution to PollExecutionResponse with specific journal range
    ///
    /// Similar to `to_poll_response`, but only includes journal entries from
    /// a specific event ID onwards. Useful for resuming executions or replaying
    /// from a checkpoint.
    ///
    /// # Arguments
    ///
    /// * `execution` - The claimed workflow execution
    /// * `from_event_id` - Start loading journal from this event ID (inclusive)
    ///
    /// # Returns
    ///
    /// A poll response with journal starting from the specified event ID
    pub async fn to_poll_response_from(
        &self,
        execution: &WorkflowExecution,
        from_event_id: i64,
    ) -> OrcherResult<PollExecutionResponse> {
        debug!(
            execution_id = %execution.execution_id,
            from_event_id = from_event_id,
            "Converting execution to poll response (from event ID)"
        );

        // Get full response first
        let mut response = self.to_poll_response(execution).await?;

        // Filter journal to only include entries >= from_event_id
        response
            .journal
            .retain(|entry| entry.event_id >= from_event_id);

        debug!(
            execution_id = %response.execution_id,
            journal_entries = response.journal.len(),
            from_event_id = from_event_id,
            "Filtered journal to specified range"
        );

        Ok(response)
    }

    /// Get journal entry count for an execution
    ///
    /// Utility method to check how many journal entries exist without
    /// loading them all into memory.
    ///
    /// # Arguments
    ///
    /// * `workflow_id` - Workflow identifier
    /// * `execution_id` - Execution identifier
    ///
    /// # Returns
    ///
    /// Number of journal entries for the execution
    pub async fn get_journal_count(
        &self,
        workflow_id: &str,
        execution_id: &str,
    ) -> OrcherResult<usize> {
        let journal = self
            .journal_repo
            .get_journal(workflow_id, execution_id)
            .await?;

        Ok(journal.len())
    }
}

/// The attempt a run's activations carry: counted from 1, as a task's are.
///
/// A run's retry count is how many workflow-level retries came before it — 0
/// for the first run, 1 for the run of its first retry — so its attempt is one
/// more. The count is fixed when the run is created and never changes, so
/// every activation of a run, replays included, reads the same attempt.
pub(crate) fn workflow_attempt(retry_count: i32) -> i32 {
    retry_count.max(0).saturating_add(1)
}

#[cfg(test)]
mod tests {
    use super::*;
    use jiff::SignedDuration;
    use orcher_common::types::time::Timestamptz;
    use orcher_storage::schema::types::{execution::WorkflowExecution, journal::JournalEntry};
    use sqlx::PgPool;
    use uuid::Uuid;

    #[test]
    fn a_runs_attempt_counts_from_one() {
        assert_eq!(workflow_attempt(0), 1, "the first run");
        assert_eq!(workflow_attempt(1), 2, "the run of the first retry");
        assert_eq!(workflow_attempt(4), 5);
        assert_eq!(workflow_attempt(-1), 1, "a count below 0 reads as none");
        assert_eq!(workflow_attempt(i32::MAX), i32::MAX);
    }

    async fn create_test_pool() -> PgPool {
        let database_url = std::env::var("DATABASE_URL")
            .unwrap_or_else(|_| "postgres://orcher:orcher@localhost/orcher_test".to_string());

        PgPool::connect(&database_url)
            .await
            .expect("Failed to connect to test database")
    }

    #[tokio::test]
    #[ignore] // Requires database
    async fn test_to_poll_response_success() {
        let pool = create_test_pool().await;
        let journal_repo = Arc::new(JournalRepository::new(pool.clone()));
        let converter = Converter::new(journal_repo.clone());

        // Create an execution
        let exec_id = format!("test_exec_{}", Uuid::new_v4());
        let workflow_id = "test_workflow".to_string();

        let mut execution = WorkflowExecution::builder()
            .execution_id(exec_id.clone())
            .workflow_name(workflow_id.clone())
            .workflow_version("v1.0".to_string())
            .namespace("test".to_string())
            .parameters(serde_json::json!({}))
            .build();

        execution.task_queue = "test-queue".to_string();
        execution.claimed_by = Some("service-1".to_string());
        execution.claimed_at = Some(Timestamptz::now());
        execution.claim_timeout_at = Some(Timestamptz::now() + SignedDuration::from_secs(300));
        execution.retry_count = 0;

        // Create journal entries
        let entry1 = JournalEntry::new(
            workflow_id.clone(),
            exec_id.clone(),
            1,
            "WorkflowExecutionStarted".to_string(),
            serde_json::json!({"workflow_id": workflow_id}),
        );

        journal_repo.append(entry1).await.unwrap();

        let entry2 = JournalEntry::new(
            workflow_id.clone(),
            exec_id.clone(),
            2,
            "TaskScheduled".to_string(),
            serde_json::json!({"task_id": "task-1"}),
        );

        journal_repo.append(entry2).await.unwrap();

        // Convert to poll response
        let response = converter.to_poll_response(&execution).await.unwrap();

        assert_eq!(response.execution_id, exec_id);
        assert_eq!(response.workflow_id, workflow_id);
        assert_eq!(response.task_queue, "test-queue");
        assert_eq!(response.namespace, "test");
        assert_eq!(response.journal.len(), 2);
        assert_eq!(response.started_event_id, 1);
        assert_eq!(response.attempt, 1, "a first run is attempt 1");
        assert_eq!(response.claimed_by, "service-1");
    }

    #[tokio::test]
    #[ignore] // Requires database
    async fn test_to_poll_response_from() {
        let pool = create_test_pool().await;
        let journal_repo = Arc::new(JournalRepository::new(pool.clone()));
        let converter = Converter::new(journal_repo.clone());

        let exec_id = format!("test_exec_{}", Uuid::new_v4());
        let workflow_id = "test_workflow".to_string();

        let mut execution = WorkflowExecution::builder()
            .execution_id(exec_id.clone())
            .workflow_name(workflow_id.clone())
            .workflow_version("v1.0".to_string())
            .namespace("test".to_string())
            .parameters(serde_json::json!({}))
            .build();

        execution.task_queue = "test-queue".to_string();
        execution.claimed_by = Some("service-1".to_string());
        execution.claimed_at = Some(Timestamptz::now());
        execution.claim_timeout_at = Some(Timestamptz::now() + SignedDuration::from_secs(300));

        // Create 5 journal entries
        for i in 1..=5 {
            let entry = JournalEntry::new(
                workflow_id.clone(),
                exec_id.clone(),
                i,
                format!("Event{}", i),
                serde_json::json!({"event_id": i}),
            );
            journal_repo.append(entry).await.unwrap();
        }

        // Get response from event 3 onwards
        let response = converter
            .to_poll_response_from(&execution, 3)
            .await
            .unwrap();

        assert_eq!(response.journal.len(), 3); // Events 3, 4, 5
        assert_eq!(response.journal[0].event_id, 3);
        assert_eq!(response.journal[1].event_id, 4);
        assert_eq!(response.journal[2].event_id, 5);
    }

    #[tokio::test]
    #[ignore] // Requires database
    async fn test_get_journal_count() {
        let pool = create_test_pool().await;
        let journal_repo = Arc::new(JournalRepository::new(pool.clone()));
        let converter = Converter::new(journal_repo.clone());

        let exec_id = format!("test_exec_{}", Uuid::new_v4());
        let workflow_id = "test_workflow".to_string();

        // Create 3 journal entries
        for i in 1..=3 {
            let entry = JournalEntry::new(
                workflow_id.clone(),
                exec_id.clone(),
                i,
                format!("Event{}", i),
                serde_json::json!({"event_id": i}),
            );
            journal_repo.append(entry).await.unwrap();
        }

        let count = converter
            .get_journal_count(&workflow_id, &exec_id)
            .await
            .unwrap();

        assert_eq!(count, 3);
    }

    #[tokio::test]
    #[ignore] // Requires database
    async fn test_journal_entry_conversion() {
        let pool = create_test_pool().await;
        let journal_repo = Arc::new(JournalRepository::new(pool.clone()));
        let converter = Converter::new(journal_repo.clone());

        let exec_id = format!("test_exec_{}", Uuid::new_v4());
        let workflow_id = "test_workflow".to_string();

        let mut execution = WorkflowExecution::builder()
            .execution_id(exec_id.clone())
            .workflow_name(workflow_id.clone())
            .workflow_version("v1.0".to_string())
            .namespace("test".to_string())
            .parameters(serde_json::json!({}))
            .build();

        execution.task_queue = "test-queue".to_string();
        execution.claimed_by = Some("service-1".to_string());
        execution.claimed_at = Some(Timestamptz::now());
        execution.claim_timeout_at = Some(Timestamptz::now() + SignedDuration::from_secs(300));

        // Create journal entry with specific attributes
        let entry = JournalEntry::new(
            workflow_id.clone(),
            exec_id.clone(),
            1,
            "WorkflowExecutionStarted".to_string(),
            serde_json::json!({
                "workflow_id": workflow_id,
                "input": {"key": "value"}
            }),
        );

        journal_repo.append(entry).await.unwrap();

        // Convert and verify
        let response = converter.to_poll_response(&execution).await.unwrap();

        assert_eq!(response.journal.len(), 1);
        let journal_entry = &response.journal[0];
        assert_eq!(journal_entry.event_id, 1);
        assert_eq!(journal_entry.entry_type, "WorkflowExecutionStarted");
        let payload = journal_entry.payload.as_ref().unwrap();
        assert_eq!(payload["workflow_id"], workflow_id);
        assert_eq!(payload["input"]["key"], "value");
    }
}
