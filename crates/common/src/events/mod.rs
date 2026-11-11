//! Shared event types for cross-crate communication.

pub mod publisher;
pub mod worker_emitter;
pub mod worker_events;

// Re-export commonly used types
pub use publisher::EventPublisher;
pub use worker_emitter::WorkerEventEmitter;
pub use worker_events::{WorkerTaskEvent, WorkerTaskEventType};
