//! gRPC service implementations.

/// Execution service for service polling and execution
/// Note: Using ORCHER terminology - "Service" not "Worker"
pub mod execution;

/// Workflow service for client workflow management API
/// Note: Using ORCHER terminology - "Service" not "Worker", "Event" not "Signal"
pub mod workflow;

/// Actor service for stateful actor operations
/// Note: Using ORCHER terminology - "Service" not "Worker"
pub mod actor;

/// Namespace service for CRUD management of namespaces
pub mod namespace;

/// Query service for searching and querying workflow executions
/// Note: Using ORCHER terminology - "Execution Journal" not "History"
pub mod query;

/// Worker registration service for lifecycle tracking
/// Handles RegisterWorker, WorkerHeartbeat, and DeregisterWorker RPCs.
pub mod worker;

/// Live query dispatch infrastructure for querying running workflows.
pub mod query_dispatch;

/// Live update dispatch infrastructure for synchronous workflow mutations.
pub mod update_dispatch;

// Re-export main service types for convenience
pub use actor::ActorServiceImpl;
pub use execution::ExecutionService;
pub use namespace::NamespaceServiceImpl;
pub use query::QueryService;
pub use worker::WorkerServiceImpl;
pub use workflow::WorkflowService;
