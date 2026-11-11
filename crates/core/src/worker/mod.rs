//! Worker registration and tracking.

pub mod liveness;
pub mod registry;
pub mod shutdowns;

pub use liveness::{PollPresence, WorkerLiveness};
pub use registry::WorkerRegistry;
pub use shutdowns::WorkerShutdowns;
