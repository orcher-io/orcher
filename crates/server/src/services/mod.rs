//! Business logic service layer.
//!
//! Core services communicate with the orchestrator via gRPC.

pub mod core;
pub mod registry;
pub mod schedule;
pub mod workflow;

pub use registry::RegistryService;
pub use schedule::ScheduleService;
