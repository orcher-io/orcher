//! Abstraction traits so dispatch modules don't depend on core's concrete types.

use async_trait::async_trait;
use orcher_common::types::dispatch::state::WorkflowExecutionState;
use orcher_common::OrcherResult;
use orcher_storage::schema::types::coordination::OutboxTask;
use orcher_storage::schema::types::journal::JournalEntry;
use std::future::Future;
use std::pin::Pin;
use uuid::Uuid;

pub type JournalLoader = Box<
    dyn FnOnce() -> Pin<Box<dyn Future<Output = OrcherResult<Vec<JournalEntry>>> + Send>> + Send,
>;

pub type IncrementalJournalLoader = Box<
    dyn FnOnce(i64) -> Pin<Box<dyn Future<Output = OrcherResult<Vec<JournalEntry>>> + Send>> + Send,
>;

/// Cache for workflow state and journal entries. Implemented by `HighPerfCache` in orcher-core.
#[async_trait]
pub trait JournalCache: Send + Sync {
    async fn get_workflow_state(&self, execution_id: &str) -> Option<WorkflowExecutionState>;
    async fn set_workflow_state(&self, execution_id: &str, state: WorkflowExecutionState);
    async fn invalidate_workflow_state(&self, execution_id: &str);

    /// Uses `incremental_loader` if entries are already cached, `full_loader` otherwise.
    async fn get_or_load_journal_incremental(
        &self,
        execution_id: &str,
        full_loader: JournalLoader,
        incremental_loader: IncrementalJournalLoader,
    ) -> OrcherResult<Vec<JournalEntry>>;

    async fn get_journal(&self, execution_id: &str) -> Option<Vec<JournalEntry>>;
    async fn set_journal(&self, execution_id: &str, entries: Vec<JournalEntry>);
    async fn invalidate_journal(&self, execution_id: &str);
    async fn invalidate_all(&self, execution_id: &str);
}

/// Outbox queue operations for task dispatch. Implemented by `OutboxQueue` in orcher-core.
#[async_trait]
pub trait TaskOutbox: Send + Sync {
    /// The oldest pending task dispatches of one `(namespace, task_queue)`, each
    /// handed to one claimer at a time. Dispatches on other queues, of the same
    /// workflow or not, are neither claimed nor waited on.
    async fn claim_dispatch_tasks(
        &self,
        organization_id: Option<Uuid>,
        batch_size: usize,
        task_queue: &str,
        namespace: &str,
    ) -> OrcherResult<Vec<OutboxTask>>;

    /// Release back to pending — e.g. a claim made as its worker shut down.
    async fn release_claimed_task(&self, task_id: Uuid) -> OrcherResult<()>;

    async fn mark_completed(&self, task_id: Uuid) -> OrcherResult<()>;
}
