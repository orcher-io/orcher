//! Event infrastructure for ORCHER: domain events, in-memory pub/sub bus,
//! publisher abstractions, persistence, and replay.
//!
//! ## Feature flags
//!
//! | Flag | What it brings in |
//! |------|-------------------|
//! | `bus` | In-memory event bus (tokio broadcast channels) |
//! | `publisher` | `EventPublisher` trait + impls (local, bridge, multi) |
//! | `metadata` | Event IDs, correlation/causation, context, timestamps |
//! | `json` | JSON serde support |
//! | `replay` | Event store + stream replay |
//! | `projections` | CQRS projection support |
//! | `storage` | PostgreSQL-backed event store + subscriptions |
//!
//! All of `bus`, `publisher`, `json`, and `metadata` are on by default.

#![allow(missing_docs)]
#![warn(clippy::all)]
#![deny(unsafe_code)]
#![cfg_attr(docsrs, feature(doc_cfg))]

pub use orcher_common::types::identifiers::{TaskId, WorkerId, WorkflowId};
pub use orcher_common::OrcherResult;

pub mod config;
pub mod error;
pub mod event;
pub use event::Event;

pub mod domain;

#[cfg(feature = "bus")]
#[cfg_attr(docsrs, doc(cfg(feature = "bus")))]
pub mod bus;

#[cfg(feature = "publisher")]
#[cfg_attr(docsrs, doc(cfg(feature = "publisher")))]
pub mod publisher;

#[cfg(feature = "metadata")]
#[cfg_attr(docsrs, doc(cfg(feature = "metadata")))]
pub mod metadata;

#[cfg(feature = "replay")]
#[cfg_attr(docsrs, doc(cfg(feature = "replay")))]
pub mod replay;

#[cfg(feature = "projections")]
#[cfg_attr(docsrs, doc(cfg(feature = "projections")))]
pub mod projections;

#[cfg(feature = "storage")]
#[cfg_attr(docsrs, doc(cfg(feature = "storage")))]
pub mod subscriptions;

/// Test helpers — public so downstream crates can use them too.
pub mod testing;

pub mod traits;

/// Convenience re-exports.
pub mod prelude {
    pub use orcher_common::types::identifiers::{TaskId, WorkerId, WorkflowId};
    pub use orcher_common::OrcherResult;

    pub use crate::error::{EventError, Result};
    pub use crate::traits::Event as EventTrait;
    pub use crate::Event;

    pub use crate::domain::{
        AlertSeverity, DlqEvent, ExecutionEvent, FailureType, SchedulerEvent, TaskEvent,
        TaskResult, WorkerEvent, WorkerTaskEvent, WorkerTaskEventType, WorkflowEvent,
    };

    #[cfg(feature = "bus")]
    pub use crate::bus::{EventBus, EventBusConfig};

    #[cfg(feature = "publisher")]
    pub use crate::publisher::{EventPublisher, LocalPublisher};

    #[cfg(feature = "metadata")]
    pub use crate::metadata::{
        CausationId, CorrelationId, EventContext, EventHeaders, EventId, EventTimestamp,
        HeaderValue, StreamId,
    };

    #[cfg(feature = "replay")]
    pub use crate::replay::{
        AppendResult, EventQuery, EventReplayer, EventStore, EventStream, ReadDirection,
        ReplayError, ReplayHandler, ReplayOptions, ReplayProgress, ReplayResult, ReplayStats,
        Snapshot, SnapshotConfig, SnapshotManager, SnapshotStore, SnapshotStrategy, StoredEvent,
        StreamConfig, StreamFilter, StreamPosition,
    };

    #[cfg(feature = "storage")]
    pub use crate::subscriptions::{
        CheckpointStore, ErrorHandlingStrategy as SubscriptionErrorStrategy,
        PostgresCheckpointStore, StreamSubscription, Subscription, SubscriptionConfig,
        SubscriptionId, SubscriptionManager, SubscriptionStatus,
    };

    #[cfg(feature = "projections")]
    pub use crate::projections::{
        PostgresProjectionStateStore, Projection, ProjectionManager, ProjectionMetadata,
        ProjectionStateStore, ProjectionStatus, TypedProjection,
    };
}

/// Crate version from Cargo.toml.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Crate name from Cargo.toml.
pub const CRATE_NAME: &str = env!("CARGO_PKG_NAME");

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_version() {
        assert!(!VERSION.is_empty());
        assert_eq!(CRATE_NAME, "orcher-events");
    }

    #[test]
    fn test_crate_compiles() {
        assert!(true);
    }
}
