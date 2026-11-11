//! Durable subscriptions with checkpoint-based resumption.

pub mod checkpoint;
pub mod manager;
pub mod stream;
pub mod types;

pub use checkpoint::{CheckpointStore, PostgresCheckpointStore};
pub use manager::SubscriptionManager;
pub use stream::StreamSubscription;
pub use types::{
    ErrorHandlingStrategy, Subscription, SubscriptionConfig, SubscriptionId, SubscriptionStatus,
};

use crate::replay::ReplayError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum SubscriptionError {
    #[error("Subscription not found: {0}")]
    NotFound(String),

    #[error("Subscription already exists: {0}")]
    AlreadyExists(String),

    #[error("Checkpoint error: {0}")]
    CheckpointError(String),

    #[error("Stream error: {0}")]
    StreamError(String),

    #[error("Invalid state: expected {expected}, got {actual}")]
    InvalidState { expected: String, actual: String },

    #[error("Event processing failed: {0}")]
    ProcessingError(String),

    #[error("Replay error: {0}")]
    ReplayError(#[from] ReplayError),

    #[error("Internal error: {0}")]
    Internal(String),
}

impl From<SubscriptionError> for crate::error::EventError {
    fn from(err: SubscriptionError) -> Self {
        crate::error::EventError::internal(err.to_string())
    }
}

pub type Result<T> = std::result::Result<T, SubscriptionError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_subscription_error_display() {
        let err = SubscriptionError::NotFound("test-sub".to_string());
        assert!(err.to_string().contains("test-sub"));

        let err = SubscriptionError::InvalidState {
            expected: "Running".to_string(),
            actual: "Stopped".to_string(),
        };
        assert!(err.to_string().contains("Running"));
        assert!(err.to_string().contains("Stopped"));
    }

    #[test]
    fn test_subscription_error_conversion() {
        let sub_err = SubscriptionError::ProcessingError("test".to_string());
        let event_err: crate::error::EventError = sub_err.into();
        assert!(event_err.to_string().contains("test"));
    }
}
