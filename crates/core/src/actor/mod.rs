//! Server-side actor coordination (routing, state, service registry).

pub mod router;
pub mod service_registry;
pub mod state_storage;

// Re-export main types
pub use router::ActorRouter;
pub use service_registry::{ServiceInfo, ServiceRegistry};
pub use state_storage::{ActorStateStorage, StateStorage};

// Note: ActorServiceImpl is in crate::grpc::services::actor following core crate patterns
// Import it via: use orcher_core::grpc::services::ActorServiceImpl;
