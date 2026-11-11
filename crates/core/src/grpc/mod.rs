//! gRPC service layer.

pub mod service_auth;
pub mod streaming;

/// Cached authentication validator for API key validation
pub mod auth;

/// Tower Layer for gRPC request authentication
pub mod auth_layer;

/// gRPC service implementations (servers)
pub mod services;

/// Carrying the calling tenant across a gRPC hop, so namespace lookups can be
/// scoped to the organisation that asked. Optional: single-tenant deployments
/// send nothing and behave exactly as before.
pub mod task_token;
pub mod tenant;

/// Tying a workflow completion to the activation it answers.
pub mod activation_token;

/// What every outgoing call carries: the tenant above, plus this process's
/// own credential when the orchestrator it calls requires one.
pub mod outbound;

/// Relaying a request to the cluster node that owns its workflow.
pub mod forward;
pub mod loopback;

pub mod payload_limits;

/// The run a request names: one scoped resolution for every RPC that
/// addresses a workflow run.
pub mod runs;

// Re-export service implementations (modern pull-based services only)
pub use services::{ActorServiceImpl, ExecutionService, WorkflowService};
