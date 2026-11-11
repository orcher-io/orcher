//! Event publishing: trait + implementations.
//!
//! - [`LocalPublisher`] — straight to the in-memory bus
//! - [`BridgePublisher`] — bus + optional persistence (background or inline)
//! - [`MultiPublisher`] — fan-out to multiple publishers with configurable strategy
//! - [`MockPublisher`] — records events for test assertions

pub mod bridge;
pub mod local;
pub mod multi;
pub mod testing;
pub mod traits;

pub use bridge::{BridgeConfig, BridgePublisher};
pub use local::LocalPublisher;
pub use multi::{MultiPublisher, PublishStrategy};
pub use testing::MockPublisher;
pub use traits::{DomainEventPublisher, EventPublisher, IntegrationEventPublisher};
