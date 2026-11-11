//! CQRS projections -- build read models from event streams.

pub mod manager;
pub mod state;
pub mod traits;

pub use manager::ProjectionManager;
pub use state::{PostgresProjectionStateStore, ProjectionStateStore};
pub use traits::{Projection, ProjectionMetadata, ProjectionStatus, TypedProjection};

use thiserror::Error;

#[derive(Debug, Error)]
pub enum ProjectionError {
    #[error("Projection not found: {0}")]
    NotFound(String),

    #[error("Projection already exists: {0}")]
    AlreadyExists(String),

    #[error("State error: {0}")]
    StateError(String),

    #[error("Projection failed: {0}")]
    ProjectionFailed(String),

    #[error("Serialization error: {0}")]
    SerializationError(String),

    #[error("Deserialization error: {0}")]
    DeserializationError(String),

    #[error("Subscription error: {0}")]
    SubscriptionError(String),

    #[error("Internal error: {0}")]
    Internal(String),
}

impl From<ProjectionError> for crate::error::EventError {
    fn from(err: ProjectionError) -> Self {
        crate::error::EventError::internal(err.to_string())
    }
}

pub type Result<T> = std::result::Result<T, ProjectionError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_projection_error_display() {
        let err = ProjectionError::NotFound("test-projection".to_string());
        assert!(err.to_string().contains("test-projection"));

        let err = ProjectionError::ProjectionFailed("test error".to_string());
        assert!(err.to_string().contains("test error"));
    }

    #[test]
    fn test_projection_error_conversion() {
        let proj_err = ProjectionError::StateError("test".to_string());
        let event_err: crate::error::EventError = proj_err.into();
        assert!(event_err.to_string().contains("test"));
    }
}
