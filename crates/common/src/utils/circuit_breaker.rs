//! Circuit breaker for fault tolerance with configurable thresholds and recovery.

use crate::error::OrcherError;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;

/// Circuit breaker state
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CircuitState {
    /// Circuit is closed - requests flow normally
    Closed,
    /// Circuit is open - requests are immediately failed
    Open,
    /// Circuit is half-open - testing if service has recovered
    HalfOpen,
}

/// Circuit breaker configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CircuitBreakerConfig {
    /// Failures before opening the circuit.
    pub failure_threshold: u32,
    /// Wait time before transitioning Open -> HalfOpen.
    pub recovery_timeout: Duration,
    /// Successes in HalfOpen needed to close the circuit.
    pub success_threshold: u32,
    /// Rolling window for counting failures.
    pub failure_window: Duration,
    /// Max concurrent requests allowed in HalfOpen.
    pub half_open_max_requests: u32,
}

/// Circuit breaker statistics
#[derive(Debug, Clone)]
pub struct CircuitStats {
    pub state: CircuitState,
    pub total_requests: u64,
    pub success_count: u64,
    pub failure_count: u64,
    pub rejected_count: u64,
    pub last_state_change: SystemTime,
    pub last_opened: Option<SystemTime>,
}

struct InternalCircuitState {
    state: CircuitState,
    failure_count: u32,
    success_count: u32,
    last_failure_time: Option<Instant>,
    last_state_change: Instant,
    half_open_requests: u32,
}

pub struct CircuitBreaker {
    config: CircuitBreakerConfig,
    state: Arc<RwLock<InternalCircuitState>>,
    stats: Arc<CircuitBreakerStats>,
}

struct CircuitBreakerStats {
    total_requests: AtomicU64,
    success_count: AtomicU64,
    failure_count: AtomicU64,
    rejected_count: AtomicU64,
    last_state_change: AtomicU64,
    last_opened: AtomicU64,
}

impl CircuitBreaker {
    pub fn new(config: CircuitBreakerConfig) -> Self {
        let now = Instant::now();
        Self {
            config,
            state: Arc::new(RwLock::new(InternalCircuitState {
                state: CircuitState::Closed,
                failure_count: 0,
                success_count: 0,
                last_failure_time: None,
                last_state_change: now,
                half_open_requests: 0,
            })),
            stats: Arc::new(CircuitBreakerStats {
                total_requests: AtomicU64::new(0),
                success_count: AtomicU64::new(0),
                failure_count: AtomicU64::new(0),
                rejected_count: AtomicU64::new(0),
                last_state_change: AtomicU64::new(
                    SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs(),
                ),
                last_opened: AtomicU64::new(0),
            }),
        }
    }

    pub async fn execute<T, F, Fut>(&self, operation: F) -> Result<T, OrcherError>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<T, OrcherError>>,
    {
        self.stats.total_requests.fetch_add(1, Ordering::Relaxed);

        if !self.should_allow_request().await {
            self.stats.rejected_count.fetch_add(1, Ordering::Relaxed);
            return Err(OrcherError::RateLimit {
                message: "Circuit breaker is open - request rejected".to_string(),
                retry_after_secs: Some(self.config.recovery_timeout.as_secs()),
            });
        }

        let result = operation().await;

        match &result {
            Ok(_) => self.record_success().await,
            Err(_) => self.record_failure().await,
        }

        result
    }

    async fn should_allow_request(&self) -> bool {
        let mut state = self.state.write().await;
        let now = Instant::now();

        match state.state {
            CircuitState::Closed => true,
            CircuitState::Open => {
                if now.duration_since(state.last_state_change) >= self.config.recovery_timeout {
                    state.state = CircuitState::HalfOpen;
                    state.success_count = 0;
                    state.half_open_requests = 0;
                    state.last_state_change = now;
                    self.update_stats_state_change(CircuitState::HalfOpen);
                    true
                } else {
                    false
                }
            }
            CircuitState::HalfOpen => {
                if state.half_open_requests < self.config.half_open_max_requests {
                    state.half_open_requests += 1;
                    true
                } else {
                    false
                }
            }
        }
    }

    async fn record_success(&self) {
        self.stats.success_count.fetch_add(1, Ordering::Relaxed);
        let mut state = self.state.write().await;

        match state.state {
            CircuitState::Closed => {
                state.failure_count = 0;
                state.last_failure_time = None;
            }
            CircuitState::HalfOpen => {
                state.success_count += 1;
                if state.success_count >= self.config.success_threshold {
                    state.state = CircuitState::Closed;
                    state.failure_count = 0;
                    state.success_count = 0;
                    state.half_open_requests = 0;
                    state.last_state_change = Instant::now();
                    state.last_failure_time = None;
                    self.update_stats_state_change(CircuitState::Closed);
                }
            }
            CircuitState::Open => {
                // This shouldn't happen, but handle gracefully
            }
        }
    }

    async fn record_failure(&self) {
        self.stats.failure_count.fetch_add(1, Ordering::Relaxed);
        let mut state = self.state.write().await;
        let now = Instant::now();

        match state.state {
            CircuitState::Closed => {
                state.failure_count += 1;
                state.last_failure_time = Some(now);

                if state.failure_count >= self.config.failure_threshold {
                    if let Some(last_failure) = state.last_failure_time {
                        if now.duration_since(last_failure) <= self.config.failure_window {
                            state.state = CircuitState::Open;
                            state.last_state_change = now;
                            self.update_stats_state_change(CircuitState::Open);
                            self.stats.last_opened.store(
                                SystemTime::now()
                                    .duration_since(UNIX_EPOCH)
                                    .unwrap_or_default()
                                    .as_secs(),
                                Ordering::Relaxed,
                            );
                        }
                    }
                }
            }
            CircuitState::HalfOpen => {
                state.state = CircuitState::Open;
                state.failure_count = 1;
                state.success_count = 0;
                state.half_open_requests = 0;
                state.last_state_change = now;
                state.last_failure_time = Some(now);
                self.update_stats_state_change(CircuitState::Open);
                self.stats.last_opened.store(
                    SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs(),
                    Ordering::Relaxed,
                );
            }
            CircuitState::Open => {
                state.last_failure_time = Some(now);
            }
        }
    }

    pub async fn stats(&self) -> CircuitStats {
        let state = self.state.read().await;
        let last_opened = self.stats.last_opened.load(Ordering::Relaxed);

        CircuitStats {
            state: state.state,
            total_requests: self.stats.total_requests.load(Ordering::Relaxed),
            success_count: self.stats.success_count.load(Ordering::Relaxed),
            failure_count: self.stats.failure_count.load(Ordering::Relaxed),
            rejected_count: self.stats.rejected_count.load(Ordering::Relaxed),
            last_state_change: UNIX_EPOCH
                + Duration::from_secs(self.stats.last_state_change.load(Ordering::Relaxed)),
            last_opened: if last_opened > 0 {
                Some(UNIX_EPOCH + Duration::from_secs(last_opened))
            } else {
                None
            },
        }
    }

    pub async fn current_state(&self) -> CircuitState {
        let state = self.state.read().await;
        state.state
    }

    /// Manually open the circuit (for testing or emergency).
    pub async fn force_open(&self) {
        let mut state = self.state.write().await;
        state.state = CircuitState::Open;
        state.last_state_change = Instant::now();
        self.update_stats_state_change(CircuitState::Open);
    }

    /// Manually close the circuit (for testing or recovery).
    pub async fn force_close(&self) {
        let mut state = self.state.write().await;
        state.state = CircuitState::Closed;
        state.failure_count = 0;
        state.success_count = 0;
        state.half_open_requests = 0;
        state.last_failure_time = None;
        state.last_state_change = Instant::now();
        self.update_stats_state_change(CircuitState::Closed);
    }

    pub async fn reset_stats(&self) {
        self.stats.total_requests.store(0, Ordering::Relaxed);
        self.stats.success_count.store(0, Ordering::Relaxed);
        self.stats.failure_count.store(0, Ordering::Relaxed);
        self.stats.rejected_count.store(0, Ordering::Relaxed);
        self.stats.last_opened.store(0, Ordering::Relaxed);
    }

    fn update_stats_state_change(&self, _new_state: CircuitState) {
        self.stats.last_state_change.store(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            Ordering::Relaxed,
        );
    }
}

impl Default for CircuitBreakerConfig {
    fn default() -> Self {
        Self {
            failure_threshold: 5,
            recovery_timeout: Duration::from_secs(60),
            success_threshold: 3,
            failure_window: Duration::from_secs(60),
            half_open_max_requests: 3,
        }
    }
}

impl fmt::Display for CircuitState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CircuitState::Closed => write!(f, "closed"),
            CircuitState::Open => write!(f, "open"),
            CircuitState::HalfOpen => write!(f, "half-open"),
        }
    }
}

pub fn create_circuit_breaker() -> CircuitBreaker {
    CircuitBreaker::new(CircuitBreakerConfig::default())
}

pub fn create_circuit_breaker_with_threshold(failure_threshold: u32) -> CircuitBreaker {
    CircuitBreaker::new(CircuitBreakerConfig {
        failure_threshold,
        ..Default::default()
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_circuit_breaker_closed_state() {
        let circuit = create_circuit_breaker();

        // Should allow requests in closed state
        let result = circuit
            .execute(|| async { Ok::<i32, OrcherError>(42) })
            .await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 42);
        assert_eq!(circuit.current_state().await, CircuitState::Closed);
    }

    #[tokio::test]
    async fn test_circuit_breaker_opens_on_failures() {
        let config = CircuitBreakerConfig {
            failure_threshold: 2,
            recovery_timeout: Duration::from_millis(100),
            success_threshold: 1,
            failure_window: Duration::from_secs(60),
            half_open_max_requests: 1,
        };
        let circuit = CircuitBreaker::new(config);

        // First failure
        let _ = circuit
            .execute(|| async {
                Err::<i32, OrcherError>(OrcherError::Internal {
                    message: "test failure".to_string(),
                    source: None,
                })
            })
            .await;
        assert_eq!(circuit.current_state().await, CircuitState::Closed);

        // Second failure should open the circuit
        let _ = circuit
            .execute(|| async {
                Err::<i32, OrcherError>(OrcherError::Internal {
                    message: "test failure".to_string(),
                    source: None,
                })
            })
            .await;
        assert_eq!(circuit.current_state().await, CircuitState::Open);
    }

    #[tokio::test]
    async fn test_circuit_breaker_rejects_when_open() {
        let circuit = create_circuit_breaker_with_threshold(1);

        // Cause a failure to open the circuit
        let _ = circuit
            .execute(|| async {
                Err::<i32, OrcherError>(OrcherError::Internal {
                    message: "test failure".to_string(),
                    source: None,
                })
            })
            .await;

        // Next request should be rejected
        let result = circuit
            .execute(|| async { Ok::<i32, OrcherError>(42) })
            .await;

        assert!(result.is_err());
        if let Err(OrcherError::RateLimit { message, .. }) = result {
            assert!(message.contains("Circuit breaker is open"));
        } else {
            panic!("Expected RateLimit error");
        }
    }

    #[tokio::test]
    async fn test_circuit_breaker_half_open_recovery() {
        let config = CircuitBreakerConfig {
            failure_threshold: 1,
            recovery_timeout: Duration::from_millis(50),
            success_threshold: 1,
            failure_window: Duration::from_secs(60),
            half_open_max_requests: 1,
        };
        let circuit = CircuitBreaker::new(config);

        // Cause a failure to open the circuit
        let _ = circuit
            .execute(|| async {
                Err::<i32, OrcherError>(OrcherError::Internal {
                    message: "test failure".to_string(),
                    source: None,
                })
            })
            .await;
        assert_eq!(circuit.current_state().await, CircuitState::Open);

        // Wait for recovery timeout
        tokio::time::sleep(Duration::from_millis(60)).await;

        // Next request should transition to half-open and succeed
        let result = circuit
            .execute(|| async { Ok::<i32, OrcherError>(42) })
            .await;

        assert!(result.is_ok());
        assert_eq!(circuit.current_state().await, CircuitState::Closed);
    }

    #[tokio::test]
    async fn test_circuit_breaker_statistics() {
        let circuit = create_circuit_breaker();

        // Execute some operations
        let _ = circuit
            .execute(|| async { Ok::<i32, OrcherError>(1) })
            .await;
        let _ = circuit
            .execute(|| async {
                Err::<i32, OrcherError>(OrcherError::Internal {
                    message: "failure".to_string(),
                    source: None,
                })
            })
            .await;

        let stats = circuit.stats().await;
        assert_eq!(stats.total_requests, 2);
        assert_eq!(stats.success_count, 1);
        assert_eq!(stats.failure_count, 1);
        assert_eq!(stats.rejected_count, 0);
    }

    #[tokio::test]
    async fn test_force_operations() {
        let circuit = create_circuit_breaker();

        // Force open
        circuit.force_open().await;
        assert_eq!(circuit.current_state().await, CircuitState::Open);

        // Force close
        circuit.force_close().await;
        assert_eq!(circuit.current_state().await, CircuitState::Closed);
    }

    #[test]
    fn test_circuit_state_display() {
        assert_eq!(CircuitState::Closed.to_string(), "closed");
        assert_eq!(CircuitState::Open.to_string(), "open");
        assert_eq!(CircuitState::HalfOpen.to_string(), "half-open");
    }

    #[test]
    fn test_default_config() {
        let config = CircuitBreakerConfig::default();
        assert_eq!(config.failure_threshold, 5);
        assert_eq!(config.recovery_timeout, Duration::from_secs(60));
        assert_eq!(config.success_threshold, 3);
    }
}
