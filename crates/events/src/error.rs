//! Event error types.

use thiserror::Error;

/// Alias for event operation results.
pub type Result<T> = std::result::Result<T, EventError>;

/// Errors that can occur during event operations.
#[derive(Debug, Error)]
pub enum EventError {
    /// Publishing failed.
    #[error("Failed to publish event: {message}")]
    PublishError {
        /// Details about what went wrong.
        message: String,
    },

    /// Subscription failed.
    #[error("Failed to subscribe to events: {message}")]
    SubscriptionError {
        /// Details about what went wrong.
        message: String,
    },

    /// Serialization failed.
    #[error("Failed to serialize event: {message}")]
    SerializationError {
        /// Details about what went wrong.
        message: String,
    },

    /// Deserialization failed.
    #[error("Failed to deserialize event: {message}")]
    DeserializationError {
        /// Details about what went wrong.
        message: String,
    },

    /// Validation failed.
    #[error("Event validation failed: {message}")]
    ValidationError {
        /// Details about what went wrong.
        message: String,
    },

    /// Something went wrong in the event bus.
    #[error("Event bus error: {message}")]
    BusError {
        /// Details about what went wrong.
        message: String,
    },

    /// An event handler returned an error.
    #[error("Event handler error: {message}")]
    HandlerError {
        /// Details about what went wrong.
        message: String,
    },

    /// Middleware processing failed.
    #[error("Event middleware error: {message}")]
    MiddlewareError {
        /// Details about what went wrong.
        message: String,
    },

    /// Storage layer error (DB, I/O, etc).
    #[error("Event storage error: {message}")]
    StorageError {
        /// Details about what went wrong.
        message: String,
    },

    /// Catch-all for unexpected internal failures.
    #[error("Internal error: {message}")]
    Internal {
        /// Details about what went wrong.
        message: String,
    },

    /// Unclassified error with a freeform message.
    #[error("{0}")]
    Other(String),
}

impl EventError {
    /// Shorthand for `PublishError`.
    pub fn publish(message: impl Into<String>) -> Self {
        Self::PublishError {
            message: message.into(),
        }
    }

    /// Shorthand for `SubscriptionError`.
    pub fn subscription(message: impl Into<String>) -> Self {
        Self::SubscriptionError {
            message: message.into(),
        }
    }

    /// Shorthand for `SerializationError`.
    pub fn serialization(message: impl Into<String>) -> Self {
        Self::SerializationError {
            message: message.into(),
        }
    }

    /// Shorthand for `DeserializationError`.
    pub fn deserialization(message: impl Into<String>) -> Self {
        Self::DeserializationError {
            message: message.into(),
        }
    }

    /// Shorthand for `ValidationError`.
    pub fn validation(message: impl Into<String>) -> Self {
        Self::ValidationError {
            message: message.into(),
        }
    }

    /// Shorthand for `BusError`.
    pub fn bus(message: impl Into<String>) -> Self {
        Self::BusError {
            message: message.into(),
        }
    }

    /// Shorthand for `HandlerError`.
    pub fn handler(message: impl Into<String>) -> Self {
        Self::HandlerError {
            message: message.into(),
        }
    }

    /// Shorthand for `MiddlewareError`.
    pub fn middleware(message: impl Into<String>) -> Self {
        Self::MiddlewareError {
            message: message.into(),
        }
    }

    /// Shorthand for `StorageError`.
    pub fn storage(message: impl Into<String>) -> Self {
        Self::StorageError {
            message: message.into(),
        }
    }

    /// Shorthand for `Internal`.
    pub fn internal(message: impl Into<String>) -> Self {
        Self::Internal {
            message: message.into(),
        }
    }

    /// Optimistic concurrency violation — the stream moved since we last checked.
    pub fn concurrency_conflict(expected: u64, actual: u64) -> Self {
        Self::StorageError {
            message: format!(
                "Concurrency conflict: expected position {}, but current position is {}",
                expected, actual
            ),
        }
    }

    /// Wraps a database error as a `StorageError`.
    pub fn database(message: impl Into<String>) -> Self {
        Self::StorageError {
            message: message.into(),
        }
    }
}

// Conversion from sqlx errors
#[cfg(feature = "storage")]
impl From<sqlx::Error> for EventError {
    fn from(err: sqlx::Error) -> Self {
        Self::StorageError {
            message: err.to_string(),
        }
    }
}

// Conversion from serde_json errors
impl From<serde_json::Error> for EventError {
    fn from(err: serde_json::Error) -> Self {
        Self::SerializationError {
            message: err.to_string(),
        }
    }
}

impl From<anyhow::Error> for EventError {
    fn from(err: anyhow::Error) -> Self {
        Self::Internal {
            message: err.to_string(),
        }
    }
}

impl From<orcher_common::error::OrcherError> for EventError {
    fn from(err: orcher_common::error::OrcherError) -> Self {
        Self::Internal {
            message: err.to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_creation() {
        let err = EventError::publish("test error");
        assert!(err.to_string().contains("test error"));
    }

    #[test]
    fn test_error_conversion() {
        let anyhow_err = anyhow::anyhow!("test error");
        let event_err: EventError = anyhow_err.into();
        assert!(matches!(event_err, EventError::Internal { .. }));
    }
}
