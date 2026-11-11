//! Strongly-typed identifier types for the ORCHER platform.

pub mod traits;

pub mod auth;
pub mod event;
pub mod node;
pub mod organization;
pub mod schedule;
pub mod task;
pub mod user;
pub mod worker;
pub mod workflow;

pub use auth::{CertificateId, CryptoKeyId, PermissionId, PolicyId, RefreshTokenId, RoleId};
pub use event::{AggregateId, CorrelationId, EventId, RequestId, SnapshotId};
pub use node::{ClusterId, NodeId};
pub use organization::{OrganizationId, OrganizationInvitationId, OrganizationMemberId};
pub use schedule::ScheduleId;
pub use task::{TaskExecutionId, TaskId};
pub use user::{ApiKeyId, SessionId, UserId};
pub use worker::WorkerId;
pub use workflow::{ExecutionId, WorkflowId};

pub use traits::{Identifier, IdentifierError, IdentifierPrefix, IdentifierValidation};
