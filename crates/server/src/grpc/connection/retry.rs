//! Retry policies for gRPC client requests
//!
//! This module provides retry logic with exponential backoff for handling
//! transient failures in gRPC communication.

use std::time::Duration;
use tokio::time::sleep;

/// Retry policy with exponential backoff
#[derive(Debug, Clone)]
pub struct RetryPolicy {
    /// Maximum number of retry attempts
    max_retries: u32,

    /// Initial backoff duration
    initial_backoff: Duration,

    /// Maximum backoff duration
    max_backoff: Duration,

    /// Backoff multiplier for exponential growth
    backoff_multiplier: f64,
}

impl RetryPolicy {
    /// Create a new retry policy
    #[must_use]
    pub fn new(max_retries: u32, initial_backoff: Duration, max_backoff: Duration) -> Self {
        Self {
            max_retries,
            initial_backoff,
            max_backoff,
            backoff_multiplier: 2.0,
        }
    }

    /// Get the maximum number of retries
    #[must_use]
    pub fn max_retries(&self) -> u32 {
        self.max_retries
    }

    /// Calculate backoff duration for a given attempt
    #[must_use]
    pub fn backoff_duration(&self, attempt: u32) -> Duration {
        if attempt == 0 {
            return Duration::from_secs(0);
        }

        let backoff_secs =
            self.initial_backoff.as_secs_f64() * self.backoff_multiplier.powi(attempt as i32);

        let backoff = Duration::from_secs_f64(backoff_secs);

        // Cap at max_backoff
        if backoff > self.max_backoff {
            self.max_backoff
        } else {
            backoff
        }
    }

    /// Execute a function with retry logic
    ///
    /// # Arguments
    ///
    /// * `f` - The async function to execute with retries
    ///
    /// # Errors
    ///
    /// Returns the last error if all retry attempts fail
    pub async fn execute<F, Fut, T, E>(&self, f: F) -> Result<T, E>
    where
        F: Fn() -> Fut,
        Fut: std::future::Future<Output = Result<T, E>>,
    {
        let mut last_error = None;

        for attempt in 0..=self.max_retries {
            match f().await {
                Ok(result) => return Ok(result),
                Err(e) => {
                    last_error = Some(e);

                    // Don't sleep after the last attempt
                    if attempt < self.max_retries {
                        let backoff = self.backoff_duration(attempt + 1);
                        sleep(backoff).await;
                    }
                }
            }
        }

        Err(last_error.expect("should have at least one error"))
    }
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self::new(3, Duration::from_millis(100), Duration::from_secs(10))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_retry_policy_creation() {
        let policy = RetryPolicy::new(5, Duration::from_millis(200), Duration::from_secs(30));
        assert_eq!(policy.max_retries(), 5);
    }

    #[test]
    fn test_default_retry_policy() {
        let policy = RetryPolicy::default();
        assert_eq!(policy.max_retries(), 3);
    }

    #[test]
    fn test_backoff_duration() {
        let policy = RetryPolicy::new(5, Duration::from_millis(100), Duration::from_secs(10));

        // First attempt: no backoff
        assert_eq!(policy.backoff_duration(0), Duration::from_secs(0));

        // Second attempt: 100ms * 2^1 = 200ms
        assert_eq!(policy.backoff_duration(1), Duration::from_millis(200));

        // Third attempt: 100ms * 2^2 = 400ms
        assert_eq!(policy.backoff_duration(2), Duration::from_millis(400));

        // Fourth attempt: 100ms * 2^3 = 800ms
        assert_eq!(policy.backoff_duration(3), Duration::from_millis(800));
    }

    #[test]
    fn test_backoff_cap() {
        let policy = RetryPolicy::new(10, Duration::from_millis(100), Duration::from_secs(2));

        // Should cap at max_backoff of 2 seconds
        let backoff = policy.backoff_duration(10);
        assert_eq!(backoff, Duration::from_secs(2));
    }

    #[tokio::test]
    async fn test_execute_success_first_try() {
        let policy = RetryPolicy::default();
        let call_count = std::sync::Arc::new(std::sync::atomic::AtomicU32::new(0));
        let count_clone = call_count.clone();

        let result = policy
            .execute(|| {
                let count = count_clone.clone();
                async move {
                    count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    Ok::<_, String>(42)
                }
            })
            .await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 42);
        assert_eq!(call_count.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_execute_success_after_retries() {
        let policy = RetryPolicy::new(3, Duration::from_millis(10), Duration::from_millis(50));
        let call_count = std::sync::Arc::new(std::sync::atomic::AtomicU32::new(0));
        let count_clone = call_count.clone();

        let result = policy
            .execute(|| {
                let count = count_clone.clone();
                async move {
                    let current = count.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
                    if current < 3 {
                        Err("transient error")
                    } else {
                        Ok(42)
                    }
                }
            })
            .await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 42);
        assert_eq!(call_count.load(std::sync::atomic::Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn test_execute_all_retries_fail() {
        let policy = RetryPolicy::new(2, Duration::from_millis(10), Duration::from_millis(50));
        let call_count = std::sync::Arc::new(std::sync::atomic::AtomicU32::new(0));
        let count_clone = call_count.clone();

        let result = policy
            .execute(|| {
                let count = count_clone.clone();
                async move {
                    count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    Err::<i32, _>("persistent error")
                }
            })
            .await;

        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "persistent error");
        // Should try initial attempt + 2 retries = 3 total
        assert_eq!(call_count.load(std::sync::atomic::Ordering::SeqCst), 3);
    }
}
