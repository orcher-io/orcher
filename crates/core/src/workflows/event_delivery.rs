//! Delivering an event to a run.
//!
//! An event reaches a run the same way whoever sends it: `SendEvent`, the
//! existing-run half of `SendEventWithStart`, `BatchSendEvent`, and a batch
//! operation started over the REST API. Delivery is three steps, and a run
//! that gets the first without the other two has the event in its journal
//! and nothing to make it read it:
//!
//! 1. Journal the event in the run, once for the request id: a client that
//!    retries a send whose answer it lost delivers nothing new.
//! 2. Release the run's claim so it can be handed out again. If an activation
//!    is in flight, the release is deferred to that activation's completion,
//!    which then hands the run out again with the event in its journal.
//! 3. Wake the polls on the run's task queue, so a worker claims it now rather
//!    than at its next scan.
//!
//! A repeat (step 1 appends nothing) still does 2 and 3: the request it
//! repeats may have stopped after journaling.

use crate::engine::dispatch::PollingManager;
use crate::workflows::journal::JournalService;
use orcher_common::error::{OrcherError, OrcherResult};
use orcher_storage::repositories::domains::journal::JournalRepository;
use orcher_storage::repositories::domains::workflow_execution::repository::WorkflowExecutionRepository;
use orcher_storage::schema::types::WorkflowExecution;
use std::sync::Arc;

/// Delivers events to runs: journal, release, wake.
#[derive(Clone)]
pub struct EventDelivery {
    journal: Arc<JournalRepository>,
    executions: Arc<WorkflowExecutionRepository>,
    wake: Option<Arc<PollingManager>>,
}

impl EventDelivery {
    /// Delivery through `journal` and `executions`. Without
    /// [`with_wake`](Self::with_wake) a run is handed out at the next scan of
    /// its task queue.
    pub fn new(
        journal: Arc<JournalRepository>,
        executions: Arc<WorkflowExecutionRepository>,
    ) -> Self {
        Self {
            journal,
            executions,
            wake: None,
        }
    }

    /// Wake the polls on a run's task queue when an event is delivered to it.
    pub fn with_wake(mut self, polling: Arc<PollingManager>) -> Self {
        self.wake = Some(polling);
        self
    }

    /// Deliver the event `name` with `payload` to `run`, once for
    /// `request_id` (an empty one is never deduplicated). Answers whether the
    /// event was journaled now, rather than by an earlier request with the
    /// same id.
    ///
    /// An error after the event is journaled leaves it for the run to read
    /// when it is next handed out; the caller should report the failure, and a
    /// retry with the same request id completes the delivery without
    /// journaling it twice.
    pub async fn deliver(
        &self,
        run: &WorkflowExecution,
        name: &str,
        payload: &[u8],
        request_id: &str,
    ) -> OrcherResult<bool> {
        let journal = JournalService::new(self.journal.clone());
        let run_id = run.id.as_uuid().to_string();
        let appended = if request_id.is_empty() {
            journal
                .append_event_received(&run_id, &run.execution_id, name, payload, "")
                .await
                .map(|_| true)?
        } else {
            journal
                .append_event_received_once(&run_id, &run.execution_id, name, payload, request_id)
                .await?
        };

        self.executions
            .release_claim_to_resume(&run.execution_id)
            .await
            .map_err(|e| {
                OrcherError::internal(format!(
                    "event {name:?} is recorded in execution {}, but the run could not be \
                     released to read it: {e}",
                    run.execution_id
                ))
            })?;

        if let Some(polling) = &self.wake {
            polling
                .notify_work_available(&run.task_queue, &run.namespace)
                .await;
        }
        Ok(appended)
    }
}
