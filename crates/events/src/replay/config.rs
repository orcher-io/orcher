//! EventStore connection pooling and retry configuration.

use std::time::Duration;

/// Configuration for EventStore connection pooling and behavior.
#[derive(Debug, Clone)]
pub struct EventStoreConfig {
    pub database_url: String,
    pub max_connections: u32,
    pub min_connections: u32,
    pub connection_timeout: Duration,
    pub idle_timeout: Duration,
    pub max_lifetime: Duration,
    pub retry_config: RetryConfig,
}

impl EventStoreConfig {
    pub fn new(database_url: impl Into<String>) -> Self {
        Self {
            database_url: database_url.into(),
            ..Default::default()
        }
    }

    pub fn with_max_connections(mut self, max: u32) -> Self {
        self.max_connections = max;
        self
    }

    pub fn with_min_connections(mut self, min: u32) -> Self {
        self.min_connections = min;
        self
    }

    pub fn with_connection_timeout(mut self, timeout: Duration) -> Self {
        self.connection_timeout = timeout;
        self
    }

    pub fn with_idle_timeout(mut self, timeout: Duration) -> Self {
        self.idle_timeout = timeout;
        self
    }

    pub fn with_max_lifetime(mut self, lifetime: Duration) -> Self {
        self.max_lifetime = lifetime;
        self
    }

    pub fn with_retry_config(mut self, retry_config: RetryConfig) -> Self {
        self.retry_config = retry_config;
        self
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.database_url.is_empty() {
            return Err("database_url cannot be empty".to_string());
        }

        if self.max_connections == 0 {
            return Err("max_connections must be greater than 0".to_string());
        }

        if self.min_connections > self.max_connections {
            return Err("min_connections cannot exceed max_connections".to_string());
        }

        Ok(())
    }
}

impl Default for EventStoreConfig {
    fn default() -> Self {
        Self {
            database_url: String::new(),
            max_connections: 20,
            min_connections: 2,
            connection_timeout: Duration::from_secs(30),
            idle_timeout: Duration::from_secs(600), // 10 minutes
            max_lifetime: Duration::from_secs(3600), // 1 hour
            retry_config: RetryConfig::default(),
        }
    }
}

/// Retry logic with exponential backoff.
#[derive(Debug, Clone)]
pub struct RetryConfig {
    pub max_attempts: u32,
    pub initial_delay: Duration,
    pub max_delay: Duration,
    pub multiplier: f64,
    /// Adds +/-25% variance to retry delays.
    pub jitter: bool,
}

impl RetryConfig {
    pub fn new(max_attempts: u32) -> Self {
        Self {
            max_attempts,
            ..Default::default()
        }
    }

    pub fn with_initial_delay(mut self, delay: Duration) -> Self {
        self.initial_delay = delay;
        self
    }

    pub fn with_max_delay(mut self, delay: Duration) -> Self {
        self.max_delay = delay;
        self
    }

    pub fn with_multiplier(mut self, multiplier: f64) -> Self {
        self.multiplier = multiplier;
        self
    }

    pub fn with_jitter(mut self, jitter: bool) -> Self {
        self.jitter = jitter;
        self
    }

    /// Returns `initial_delay * multiplier^(attempt-1)`, capped at `max_delay`.
    pub fn delay_for_attempt(&self, attempt: u32) -> Duration {
        if attempt == 0 {
            return Duration::ZERO;
        }

        // Exponential backoff: initial_delay * multiplier^(attempt-1)
        let delay_ms =
            self.initial_delay.as_millis() as f64 * self.multiplier.powi((attempt - 1) as i32);

        let mut delay = Duration::from_millis(delay_ms as u64);

        // Cap at max_delay
        if delay > self.max_delay {
            delay = self.max_delay;
        }

        // Add jitter if enabled (±25% variance based on timestamp)
        if self.jitter {
            // Use timestamp for deterministic-ish jitter without rand dependency
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or(Duration::ZERO)
                .as_nanos();

            // Simple hash-like operation to get pseudo-random value
            let jitter_seed = ((now % 1000) as f64) / 1000.0; // 0.0 to 1.0
            let jitter_factor = 0.75 + (jitter_seed * 0.5); // 0.75 to 1.25
            delay = Duration::from_millis((delay.as_millis() as f64 * jitter_factor) as u64);
        }

        delay
    }

    pub fn should_retry(&self, attempt: u32) -> bool {
        attempt < self.max_attempts
    }
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            initial_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(30),
            multiplier: 2.0,
            jitter: true,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetryableError {
    Connection,
    DatabaseUnavailable,
    Deadlock,
    SerializationFailure,
}

#[cfg(feature = "storage")]
impl RetryableError {
    /// Maps sqlx errors to retryable categories based on PostgreSQL error codes.
    pub fn from_error(err: &sqlx::Error) -> Option<Self> {
        match err {
            sqlx::Error::PoolTimedOut => Some(Self::DatabaseUnavailable),
            sqlx::Error::PoolClosed => Some(Self::DatabaseUnavailable),
            sqlx::Error::Io(_) => Some(Self::Connection),
            sqlx::Error::Database(db_err) => {
                // Check PostgreSQL error codes
                if let Some(code) = db_err.code() {
                    match code.as_ref() {
                        "40001" => Some(Self::SerializationFailure), // serialization_failure
                        "40P01" => Some(Self::Deadlock),             // deadlock_detected
                        "53300" => Some(Self::DatabaseUnavailable),  // too_many_connections
                        "57P03" => Some(Self::DatabaseUnavailable),  // cannot_connect_now
                        _ => None,
                    }
                } else {
                    None
                }
            }
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = EventStoreConfig::default();
        assert_eq!(config.max_connections, 20);
        assert_eq!(config.min_connections, 2);
        assert!(config.database_url.is_empty());
    }

    #[test]
    fn test_config_builder() {
        let config = EventStoreConfig::new("postgres://localhost/test")
            .with_max_connections(50)
            .with_min_connections(5)
            .with_connection_timeout(Duration::from_secs(60));

        assert_eq!(config.database_url, "postgres://localhost/test");
        assert_eq!(config.max_connections, 50);
        assert_eq!(config.min_connections, 5);
        assert_eq!(config.connection_timeout, Duration::from_secs(60));
    }

    #[test]
    fn test_config_validation() {
        let valid_config = EventStoreConfig::new("postgres://localhost/test");
        assert!(valid_config.validate().is_ok());

        let empty_url = EventStoreConfig::default();
        assert!(empty_url.validate().is_err());

        let invalid_connections = EventStoreConfig::new("postgres://localhost/test")
            .with_max_connections(5)
            .with_min_connections(10);
        assert!(invalid_connections.validate().is_err());
    }

    #[test]
    fn test_retry_config_default() {
        let config = RetryConfig::default();
        assert_eq!(config.max_attempts, 3);
        assert_eq!(config.initial_delay, Duration::from_millis(100));
        assert_eq!(config.multiplier, 2.0);
        assert!(config.jitter);
    }

    #[test]
    fn test_retry_delay_calculation() {
        let config = RetryConfig::new(5)
            .with_initial_delay(Duration::from_millis(100))
            .with_multiplier(2.0)
            .with_jitter(false); // Disable jitter for predictable tests

        assert_eq!(config.delay_for_attempt(0), Duration::ZERO);
        assert_eq!(config.delay_for_attempt(1), Duration::from_millis(100)); // 100 * 2^0
        assert_eq!(config.delay_for_attempt(2), Duration::from_millis(200)); // 100 * 2^1
        assert_eq!(config.delay_for_attempt(3), Duration::from_millis(400)); // 100 * 2^2
    }

    #[test]
    fn test_retry_max_delay_cap() {
        let config = RetryConfig::new(10)
            .with_initial_delay(Duration::from_millis(100))
            .with_max_delay(Duration::from_millis(500))
            .with_multiplier(2.0)
            .with_jitter(false);

        // Attempt 5 would be 100 * 2^4 = 1600ms, but should be capped at 500ms
        assert_eq!(config.delay_for_attempt(5), Duration::from_millis(500));
    }

    #[test]
    fn test_should_retry() {
        let config = RetryConfig::new(3);

        assert!(config.should_retry(0));
        assert!(config.should_retry(1));
        assert!(config.should_retry(2));
        assert!(!config.should_retry(3));
        assert!(!config.should_retry(4));
    }

    #[test]
    fn test_retry_with_jitter() {
        let config = RetryConfig::new(5)
            .with_initial_delay(Duration::from_millis(100))
            .with_multiplier(2.0)
            .with_jitter(true);

        // With jitter, delays should vary within ±25%
        let delay = config.delay_for_attempt(1);
        let delay_ms = delay.as_millis() as u64;

        // Should be within 75%-125% of expected base (100ms for attempt 1)
        assert!(delay_ms >= 75, "delay_ms={}, expected >= 75", delay_ms);
        assert!(delay_ms <= 125, "delay_ms={}, expected <= 125", delay_ms);

        // Test attempt 2 (base 200ms)
        let delay = config.delay_for_attempt(2);
        let delay_ms = delay.as_millis() as u64;
        assert!(delay_ms >= 150, "delay_ms={}, expected >= 150", delay_ms);
        assert!(delay_ms <= 250, "delay_ms={}, expected <= 250", delay_ms);
    }

    #[test]
    #[cfg(feature = "storage")]
    fn test_retryable_error_detection() {
        // Pool errors should be retryable
        assert_eq!(
            RetryableError::from_error(&sqlx::Error::PoolTimedOut),
            Some(RetryableError::DatabaseUnavailable)
        );

        assert_eq!(
            RetryableError::from_error(&sqlx::Error::PoolClosed),
            Some(RetryableError::DatabaseUnavailable)
        );
    }
}
