//! In-memory pub/sub bus built on tokio broadcast channels.
//!
//! One typed channel per domain event kind, with optional per-subscription filtering.

pub mod channel;
pub mod config;
pub mod in_memory;
pub mod subscription;

pub use channel::TypedChannel;
pub use config::EventBusConfig;
pub use in_memory::EventBus;
pub use subscription::{Subscription, SubscriptionId};
