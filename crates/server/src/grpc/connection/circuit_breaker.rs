//! Circuit breaker pattern for fault tolerance
//!
//! This module provides a circuit breaker to prevent cascading failures
//! by stopping requests to a failing service temporarily.

use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Circuit breaker states
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CircuitState {
    /// Circuit is closed, requests pass through normally
    Closed,
    /// Circuit is open, requests are rejected
    Open,
    /// Circuit is half-open, testing if service recovered
    HalfOpen,
}

/// Circuit breaker for preventing cascading failures
#[derive(Clone)]
pub struct CircuitBreaker {
    /// Current failure count
    failure_count: Arc<AtomicU32>,
    /// Success count in half-open state
    success_count: Arc<AtomicU32>,
    /// Timestamp when circuit was opened (in milliseconds since epoch)
    opened_at: Arc<AtomicU64>,
    /// Failure threshold before opening circuit
    failure_threshold: u32,
    /// Timeout before attempting to close circuit
    timeout: Duration,
    /// Success threshold in half-open state before closing
    success_threshold: u32,
}

impl CircuitBreaker {
    /// Create a new circuit breaker
    ///
    /// # Arguments
    ///
    /// * `failure_threshold` - Number of failures before opening circuit
    /// * `timeout` - Duration to wait before attempting to close circuit
    #[must_use]
    pub fn new(failure_threshold: u32, timeout: Duration) -> Self {
        Self {
            failure_count: Arc::new(AtomicU32::new(0)),
            success_count: Arc::new(AtomicU32::new(0)),
            opened_at: Arc::new(AtomicU64::new(0)),
            failure_threshold,
            timeout,
            success_threshold: 2, // Require 2 successes to close
        }
    }

    /// Get the current state of the circuit breaker
    #[must_use]
    pub fn state(&self) -> CircuitState {
        let failures = self.failure_count.load(Ordering::SeqCst);
        let opened_at_ms = self.opened_at.load(Ordering::SeqCst);

        if opened_at_ms == 0 {
            // Never been opened
            return CircuitState::Closed;
        }

        if failures >= self.failure_threshold {
            // Check if timeout has elapsed
            let opened_at = Instant::now()
                .checked_sub(Duration::from_millis(
                    Instant::now()
                        .elapsed()
                        .as_millis()
                        .saturating_sub(u128::from(opened_at_ms)) as u64,
                ))
                .unwrap();
            let elapsed = Instant::now().duration_since(opened_at);

            if elapsed >= self.timeout {
                CircuitState::HalfOpen
            } else {
                CircuitState::Open
            }
        } else {
            CircuitState::Closed
        }
    }

    /// Check if the circuit breaker is open
    #[must_use]
    pub fn is_open(&self) -> bool {
        matches!(self.state(), CircuitState::Open)
    }

    /// Check if the circuit breaker allows requests
    #[must_use]
    pub fn is_call_permitted(&self) -> bool {
        !self.is_open()
    }

    /// Record a successful request
    pub fn record_success(&self) {
        let state = self.state();

        match state {
            CircuitState::HalfOpen => {
                let successes = self.success_count.fetch_add(1, Ordering::SeqCst) + 1;
                if successes >= self.success_threshold {
                    // Close the circuit
                    self.reset();
                }
            }
            CircuitState::Closed => {
                // Reset failure count on success
                self.failure_count.store(0, Ordering::SeqCst);
            }
            CircuitState::Open => {
                // Ignore successes when open
            }
        }
    }

    /// Record a failed request
    pub fn record_failure(&self) {
        let state = self.state();

        match state {
            CircuitState::Closed => {
                let failures = self.failure_count.fetch_add(1, Ordering::SeqCst) + 1;
                if failures >= self.failure_threshold {
                    self.open();
                }
            }
            CircuitState::HalfOpen => {
                // Failure in half-open state reopens the circuit
                self.open();
            }
            CircuitState::Open => {
                // Already open, no action needed
            }
        }
    }

    /// Reset the circuit breaker to closed state
    pub fn reset(&self) {
        self.failure_count.store(0, Ordering::SeqCst);
        self.success_count.store(0, Ordering::SeqCst);
        self.opened_at.store(0, Ordering::SeqCst);
    }

    /// Open the circuit breaker
    fn open(&self) {
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        self.opened_at.store(now_ms, Ordering::SeqCst);
        self.success_count.store(0, Ordering::SeqCst);
    }
}

impl Default for CircuitBreaker {
    fn default() -> Self {
        Self::new(5, Duration::from_secs(60))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_circuit_breaker_creation() {
        let cb = CircuitBreaker::new(3, Duration::from_secs(30));
        assert_eq!(cb.state(), CircuitState::Closed);
        assert!(!cb.is_open());
        assert!(cb.is_call_permitted());
    }

    #[test]
    fn test_circuit_breaker_opens_after_threshold() {
        let cb = CircuitBreaker::new(3, Duration::from_secs(30));

        // Record failures
        cb.record_failure();
        assert_eq!(cb.state(), CircuitState::Closed);

        cb.record_failure();
        assert_eq!(cb.state(), CircuitState::Closed);

        cb.record_failure();
        assert_eq!(cb.state(), CircuitState::Open);
        assert!(cb.is_open());
        assert!(!cb.is_call_permitted());
    }

    #[test]
    fn test_circuit_breaker_success_resets_failures() {
        let cb = CircuitBreaker::new(3, Duration::from_secs(30));

        cb.record_failure();
        cb.record_failure();
        assert_eq!(cb.state(), CircuitState::Closed);

        cb.record_success();
        assert_eq!(cb.state(), CircuitState::Closed);

        // Should need 3 more failures to open
        cb.record_failure();
        cb.record_failure();
        assert_eq!(cb.state(), CircuitState::Closed);
    }

    #[test]
    fn test_circuit_breaker_reset() {
        let cb = CircuitBreaker::new(3, Duration::from_secs(30));

        cb.record_failure();
        cb.record_failure();
        cb.record_failure();
        assert_eq!(cb.state(), CircuitState::Open);

        cb.reset();
        assert_eq!(cb.state(), CircuitState::Closed);
        assert!(!cb.is_open());
    }

    #[test]
    fn test_default_circuit_breaker() {
        let cb = CircuitBreaker::default();
        assert_eq!(cb.state(), CircuitState::Closed);
        assert_eq!(cb.failure_threshold, 5);
    }
}
