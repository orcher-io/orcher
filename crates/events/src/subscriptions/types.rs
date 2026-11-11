//! Core subscription types: identifiers, status, and configuration.

use orcher_common::types::time::Timestamptz;
use serde::{Deserialize, Serialize};
use std::fmt;
use uuid::Uuid;

use crate::replay::{StoredEvent, StreamPosition};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SubscriptionId(String);

impl SubscriptionId {
    pub fn generate() -> Self {
        Self(Uuid::new_v4().to_string())
    }

    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for SubscriptionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<String> for SubscriptionId {
    fn from(s: String) -> Self {
        Self(s)
    }
}

impl From<&str> for SubscriptionId {
    fn from(s: &str) -> Self {
        Self(s.to_string())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SubscriptionStatus {
    Starting,
    Running,
    Paused,
    Stopping,
    Stopped,
    Failed,
}

impl fmt::Display for SubscriptionStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Starting => write!(f, "Starting"),
            Self::Running => write!(f, "Running"),
            Self::Paused => write!(f, "Paused"),
            Self::Stopping => write!(f, "Stopping"),
            Self::Stopped => write!(f, "Stopped"),
            Self::Failed => write!(f, "Failed"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct SubscriptionConfig {
    pub start_position: StreamPosition,
    pub batch_size: usize,
    pub buffer_size: usize,
    pub checkpoint_interval: std::time::Duration,
    pub error_strategy: ErrorHandlingStrategy,
    pub auto_checkpoint: bool,
    pub read_timeout: Option<std::time::Duration>,
    /// Used for logging and checkpoint keys.
    pub name: Option<String>,
}

impl SubscriptionConfig {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_start_position(mut self, position: StreamPosition) -> Self {
        self.start_position = position;
        self
    }

    pub fn with_batch_size(mut self, size: usize) -> Self {
        self.batch_size = size;
        self
    }

    pub fn with_buffer_size(mut self, size: usize) -> Self {
        self.buffer_size = size;
        self
    }

    pub fn with_checkpoint_interval(mut self, interval: std::time::Duration) -> Self {
        self.checkpoint_interval = interval;
        self
    }

    pub fn with_error_strategy(mut self, strategy: ErrorHandlingStrategy) -> Self {
        self.error_strategy = strategy;
        self
    }

    pub fn with_auto_checkpoint(mut self, enabled: bool) -> Self {
        self.auto_checkpoint = enabled;
        self
    }

    pub fn with_read_timeout(mut self, timeout: std::time::Duration) -> Self {
        self.read_timeout = Some(timeout);
        self
    }

    pub fn with_name(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.batch_size == 0 {
            return Err("batch_size must be greater than 0".to_string());
        }

        if self.buffer_size == 0 {
            return Err("buffer_size must be greater than 0".to_string());
        }

        if self.buffer_size < self.batch_size {
            return Err("buffer_size should be at least as large as batch_size".to_string());
        }

        Ok(())
    }
}

impl Default for SubscriptionConfig {
    fn default() -> Self {
        Self {
            start_position: StreamPosition::Start,
            batch_size: 100,
            buffer_size: 1000,
            checkpoint_interval: std::time::Duration::from_secs(5),
            error_strategy: ErrorHandlingStrategy::Stop,
            auto_checkpoint: true,
            read_timeout: None,
            name: None,
        }
    }
}

/// What to do when event processing fails.
#[derive(Debug, Clone, PartialEq)]
pub enum ErrorHandlingStrategy {
    /// Stop on first error.
    Stop,
    /// Skip the failed event and continue.
    Skip,
    /// Retry with exponential backoff.
    RetryWithBackoff { max_attempts: u32 },
    /// Pause until manual intervention.
    Park,
}

impl fmt::Display for ErrorHandlingStrategy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Stop => write!(f, "Stop"),
            Self::Skip => write!(f, "Skip"),
            Self::RetryWithBackoff { max_attempts } => {
                write!(f, "RetryWithBackoff(max_attempts: {})", max_attempts)
            }
            Self::Park => write!(f, "Park"),
        }
    }
}

#[async_trait::async_trait]
pub trait Subscription: Send + Sync {
    /// Returns `None` when stopped or at end of stream.
    async fn next(&mut self) -> super::Result<Option<StoredEvent>>;

    fn id(&self) -> &SubscriptionId;
    fn current_position(&self) -> StreamPosition;
    fn status(&self) -> SubscriptionStatus;

    /// Stops processing but keeps the current position.
    async fn pause(&mut self) -> super::Result<()>;
    async fn resume(&mut self) -> super::Result<()>;

    /// Graceful shutdown -- saves the current position.
    async fn stop(&mut self) -> super::Result<()>;

    fn metadata(&self) -> SubscriptionMetadata {
        SubscriptionMetadata {
            id: self.id().clone(),
            status: self.status(),
            position: self.current_position(),
            created_at: Timestamptz::now(), // Default implementation
            last_event_at: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubscriptionMetadata {
    pub id: SubscriptionId,
    pub status: SubscriptionStatus,
    pub position: StreamPosition,
    pub created_at: Timestamptz,
    pub last_event_at: Option<Timestamptz>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_subscription_id_generation() {
        let id1 = SubscriptionId::generate();
        let id2 = SubscriptionId::generate();
        assert_ne!(id1, id2);
    }

    #[test]
    fn test_subscription_id_from_string() {
        let id = SubscriptionId::new("test-sub");
        assert_eq!(id.as_str(), "test-sub");
        assert_eq!(id.to_string(), "test-sub");
    }

    #[test]
    fn test_subscription_status_display() {
        assert_eq!(SubscriptionStatus::Running.to_string(), "Running");
        assert_eq!(SubscriptionStatus::Paused.to_string(), "Paused");
        assert_eq!(SubscriptionStatus::Failed.to_string(), "Failed");
    }

    #[test]
    fn test_subscription_config_defaults() {
        let config = SubscriptionConfig::default();
        assert_eq!(config.batch_size, 100);
        assert_eq!(config.buffer_size, 1000);
        assert!(config.auto_checkpoint);
    }

    #[test]
    fn test_subscription_config_builder() {
        let config = SubscriptionConfig::new()
            .with_batch_size(50)
            .with_buffer_size(500)
            .with_checkpoint_interval(std::time::Duration::from_secs(10))
            .with_name("test-subscription");

        assert_eq!(config.batch_size, 50);
        assert_eq!(config.buffer_size, 500);
        assert_eq!(
            config.checkpoint_interval,
            std::time::Duration::from_secs(10)
        );
        assert_eq!(config.name, Some("test-subscription".to_string()));
    }

    #[test]
    fn test_subscription_config_validation() {
        let valid = SubscriptionConfig::default();
        assert!(valid.validate().is_ok());

        let invalid_batch = SubscriptionConfig::default().with_batch_size(0);
        assert!(invalid_batch.validate().is_err());

        let invalid_buffer = SubscriptionConfig::default().with_buffer_size(0);
        assert!(invalid_buffer.validate().is_err());

        let invalid_ratio = SubscriptionConfig::default()
            .with_batch_size(100)
            .with_buffer_size(50);
        assert!(invalid_ratio.validate().is_err());
    }

    #[test]
    fn test_error_strategy_display() {
        assert_eq!(ErrorHandlingStrategy::Stop.to_string(), "Stop");
        assert_eq!(ErrorHandlingStrategy::Skip.to_string(), "Skip");
        assert_eq!(ErrorHandlingStrategy::Park.to_string(), "Park");

        let retry = ErrorHandlingStrategy::RetryWithBackoff { max_attempts: 3 };
        assert!(retry.to_string().contains("3"));
    }

    #[test]
    fn test_error_strategy_equality() {
        let stop1 = ErrorHandlingStrategy::Stop;
        let stop2 = ErrorHandlingStrategy::Stop;
        assert_eq!(stop1, stop2);

        let retry1 = ErrorHandlingStrategy::RetryWithBackoff { max_attempts: 3 };
        let retry2 = ErrorHandlingStrategy::RetryWithBackoff { max_attempts: 3 };
        let retry3 = ErrorHandlingStrategy::RetryWithBackoff { max_attempts: 5 };
        assert_eq!(retry1, retry2);
        assert_ne!(retry1, retry3);
    }
}
