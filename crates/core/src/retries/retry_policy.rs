//! Retry policy evaluation.

use jiff::SignedDuration;
use orcher_common::error::{OrcherError, OrcherResult};
use orcher_common::types::time::Timestamptz;
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tracing::{debug, info, warn};

// ================================================================================================
// RETRY POLICY TYPES
// ================================================================================================

/// Retry policy configuration stored in task_executions.retry_policy
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(from = "StoredRetryPolicy")]
pub struct RetryPolicy {
    /// Maximum number of attempts (including the initial attempt)
    /// A value of 1 means no retries
    #[serde(alias = "maximum_attempts")]
    pub max_attempts: u32,

    /// Initial retry interval in seconds (fractional — sub-second values are honored)
    #[serde(alias = "initial_interval_secs")]
    pub initial_interval_seconds: f64,

    /// Maximum retry interval in seconds (cap for backoff; fractional)
    #[serde(alias = "maximum_interval_secs")]
    pub max_interval_seconds: f64,

    /// Backoff coefficient (multiplier for each retry)
    /// For example, 2.0 means exponential backoff (double each time)
    pub backoff_coefficient: f64,

    /// Non-retryable error types (errors that should go straight to DLQ)
    ///
    /// Read under both its names — see [`non_retryable_error_types`] — and
    /// always written under this one.
    pub non_retryable_error_types: Vec<String>,
}

/// A [`RetryPolicy`] as stored, before its two spellings of the
/// non-retryable list are merged.
///
/// The SDKs' own policy type calls the list `non_retryable_errors`. Without
/// reading that name a policy in their spelling lost its list and every
/// failure was retried; reading it as a serde alias instead made a policy
/// carrying both names a duplicate-field error, and an unparsable policy
/// falls back to the default one.
#[derive(Deserialize)]
struct StoredRetryPolicy {
    #[serde(alias = "maximum_attempts")]
    max_attempts: u32,
    #[serde(alias = "initial_interval_secs")]
    initial_interval_seconds: f64,
    #[serde(alias = "maximum_interval_secs")]
    max_interval_seconds: f64,
    backoff_coefficient: f64,
    #[serde(default)]
    non_retryable_error_types: Vec<String>,
    #[serde(default)]
    non_retryable_errors: Vec<String>,
}

impl From<StoredRetryPolicy> for RetryPolicy {
    fn from(stored: StoredRetryPolicy) -> Self {
        Self {
            max_attempts: stored.max_attempts,
            initial_interval_seconds: stored.initial_interval_seconds,
            max_interval_seconds: stored.max_interval_seconds,
            backoff_coefficient: stored.backoff_coefficient,
            non_retryable_error_types: union(
                stored.non_retryable_error_types,
                stored.non_retryable_errors,
            ),
        }
    }
}

fn union(mut types: Vec<String>, more: Vec<String>) -> Vec<String> {
    for t in more {
        if !types.contains(&t) {
            types.push(t);
        }
    }
    types
}

/// The non-retryable error types of a stored retry policy, under either of
/// their names: the engine's `non_retryable_error_types` and the SDKs'
/// `non_retryable_errors`. Both lists count when both are present.
pub fn non_retryable_error_types(policy: &serde_json::Value) -> Vec<String> {
    let list = |name: &str| -> Vec<String> {
        policy
            .get(name)
            .and_then(serde_json::Value::as_array)
            .map(|types| {
                types
                    .iter()
                    .filter_map(|t| t.as_str().map(str::to_string))
                    .collect()
            })
            .unwrap_or_default()
    };
    union(
        list("non_retryable_error_types"),
        list("non_retryable_errors"),
    )
}

/// A stored task retry policy as the worker running the task is told it:
/// parsed exactly as the engine parses it when it decides a retry, so the
/// worker sees the policy that is enforced. An unparsable policy is the
/// default one, as it is for the retry decision.
pub fn stored_to_proto(stored: &serde_json::Value) -> orcher_proto::orcher::v1::RetryPolicy {
    serde_json::from_value::<RetryPolicy>(stored.clone())
        .unwrap_or_default()
        .to_proto()
}

/// Fractional seconds as a protobuf duration.
fn seconds_to_duration(seconds: f64) -> orcher_proto::prost_types::Duration {
    let seconds = seconds.max(0.0);
    orcher_proto::prost_types::Duration {
        seconds: seconds.trunc() as i64,
        nanos: (seconds.fract() * 1_000_000_000.0).round() as i32,
    }
}

impl RetryPolicy {
    /// The policy as the protocol carries it.
    pub fn to_proto(&self) -> orcher_proto::orcher::v1::RetryPolicy {
        orcher_proto::orcher::v1::RetryPolicy {
            initial_interval: Some(seconds_to_duration(self.initial_interval_seconds)),
            backoff_coefficient: self.backoff_coefficient,
            maximum_interval: Some(seconds_to_duration(self.max_interval_seconds)),
            maximum_attempts: i32::try_from(self.max_attempts).unwrap_or(i32::MAX),
            non_retryable_error_types: self.non_retryable_error_types.clone(),
        }
    }
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            initial_interval_seconds: 1.0,
            max_interval_seconds: 3600.0, // 1 hour max
            backoff_coefficient: 2.0,
            non_retryable_error_types: vec![
                "ValidationError".to_string(),
                "AuthenticationError".to_string(),
                "AuthorizationError".to_string(),
                "BadRequestError".to_string(),
            ],
        }
    }
}

impl RetryPolicy {
    /// Create a new retry policy with custom settings
    pub fn new(
        max_attempts: u32,
        initial_interval_seconds: f64,
        max_interval_seconds: f64,
        backoff_coefficient: f64,
    ) -> Self {
        Self {
            max_attempts,
            initial_interval_seconds,
            max_interval_seconds,
            backoff_coefficient,
            non_retryable_error_types: Vec::new(),
        }
    }

    /// Create a retry policy with no retries
    pub fn no_retry() -> Self {
        Self {
            max_attempts: 1,
            initial_interval_seconds: 0.0,
            max_interval_seconds: 0.0,
            backoff_coefficient: 1.0,
            non_retryable_error_types: Vec::new(),
        }
    }

    /// Create a retry policy with exponential backoff
    pub fn exponential_backoff(max_attempts: u32, initial_interval_seconds: f64) -> Self {
        Self {
            max_attempts,
            initial_interval_seconds,
            max_interval_seconds: 3600.0, // 1 hour max
            backoff_coefficient: 2.0,
            non_retryable_error_types: Vec::new(),
        }
    }

    /// Add a non-retryable error type
    pub fn with_non_retryable_error(mut self, error_type: impl Into<String>) -> Self {
        self.non_retryable_error_types.push(error_type.into());
        self
    }

    /// Calculate the backoff duration for a given attempt number
    ///
    /// # Arguments
    ///
    /// * `attempt_number` - The attempt number (1 for first retry, 2 for second, etc.)
    ///
    /// # Returns
    ///
    /// The duration to wait before the next retry attempt
    pub fn calculate_backoff(&self, attempt_number: i32) -> Duration {
        if attempt_number <= 1 {
            return Duration::from_secs_f64(self.initial_interval_seconds.max(0.0));
        }

        // Calculate exponential backoff: initial * (coefficient ^ (attempt - 1))
        let multiplier = self.backoff_coefficient.powi(attempt_number - 1);
        let interval_secs = self.initial_interval_seconds * multiplier;

        // Cap at max_interval_seconds
        let capped_secs = interval_secs.min(self.max_interval_seconds).max(0.0);

        Duration::from_secs_f64(capped_secs)
    }

    /// Check if an error type is retryable
    pub fn is_error_retryable(&self, error_type: Option<&str>) -> bool {
        if let Some(error_type) = error_type {
            !self
                .non_retryable_error_types
                .contains(&error_type.to_string())
        } else {
            true // Unknown errors are retryable by default
        }
    }

    /// Check if the task has attempts remaining
    pub fn has_attempts_remaining(&self, current_attempt: i32) -> bool {
        if self.max_attempts == 0 {
            // 0 = unlimited retries
            return true;
        }
        current_attempt < self.max_attempts as i32
    }
}

// ================================================================================================
// RETRY DECISION
// ================================================================================================

/// Decision result from retry policy evaluation
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RetryDecision {
    /// Task should be retried with the given backoff duration
    Retry {
        /// Duration to wait before scheduling the retry
        backoff_duration: Duration,
        /// The next attempt number
        next_attempt: i32,
        /// Scheduled time for the retry (current time + backoff)
        scheduled_at: Timestamptz,
    },
    /// Task should be moved to the dead letter queue
    MoveToDlq {
        /// Reason for moving to DLQ
        reason: DlqReason,
    },
}

/// Reasons for moving a task to the dead letter queue
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum DlqReason {
    /// Maximum retry attempts exhausted
    MaxAttemptsExceeded {
        /// Maximum number of retry attempts allowed
        max_attempts: u32,
        /// Current attempt number
        current_attempt: i32,
    },
    /// Error type is non-retryable
    NonRetryableError {
        /// The type of error that occurred
        error_type: String,
    },
    /// Task has no retry policy configured
    NoRetryPolicy,
    /// Retry policy is invalid or malformed
    InvalidRetryPolicy {
        /// Description of the validation error
        error: String,
    },
    /// The task's workflow was cancelled, or asked to cancel and the task is
    /// not cleanup scheduled in answer: the work is interrupted, and a retry
    /// would only run it again
    StoppedByCancellation,
    /// The task was pinned to a session whose worker is gone: only that
    /// worker polls the session's queue, so a retry there would never run
    SessionLost,
}

impl std::fmt::Display for DlqReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DlqReason::MaxAttemptsExceeded {
                max_attempts,
                current_attempt,
            } => write!(
                f,
                "Maximum retry attempts exceeded: {}/{} attempts",
                current_attempt, max_attempts
            ),
            DlqReason::NonRetryableError { error_type } => {
                write!(f, "Non-retryable error type: {}", error_type)
            }
            DlqReason::NoRetryPolicy => write!(f, "No retry policy configured"),
            DlqReason::InvalidRetryPolicy { error } => {
                write!(f, "Invalid retry policy: {}", error)
            }
            DlqReason::StoppedByCancellation => {
                write!(f, "Stopped by its workflow's cancellation")
            }
            DlqReason::SessionLost => {
                write!(
                    f,
                    "Its session's worker is gone, and only that worker could run it"
                )
            }
        }
    }
}

// ================================================================================================
// RETRY POLICY EVALUATOR
// ================================================================================================

/// Service for evaluating retry policies and making retry decisions
#[derive(Debug, Clone)]
pub struct RetryPolicyEvaluator {
    /// Default retry policy to use when none is specified
    default_policy: RetryPolicy,
}

impl RetryPolicyEvaluator {
    /// Create a new retry policy evaluator with default settings
    pub fn new() -> Self {
        Self {
            default_policy: RetryPolicy::default(),
        }
    }

    /// Create a new retry policy evaluator with a custom default policy
    pub fn with_default_policy(default_policy: RetryPolicy) -> Self {
        Self { default_policy }
    }

    /// Evaluate whether a failed task should be retried or moved to DLQ
    ///
    /// # Arguments
    ///
    /// * `current_attempt` - The current attempt number
    /// * `retry_policy_json` - The retry policy as JSON (from task_executions.retry_policy)
    /// * `error_type` - The error type from the failure
    ///
    /// # Returns
    ///
    /// A `RetryDecision` indicating whether to retry or move to DLQ
    pub fn evaluate_retry(
        &self,
        current_attempt: i32,
        retry_policy_json: Option<&serde_json::Value>,
        error_type: Option<&str>,
    ) -> OrcherResult<RetryDecision> {
        debug!(
            "Evaluating retry for attempt {}, error_type: {:?}",
            current_attempt, error_type
        );

        // Parse retry policy or use default
        let retry_policy = match retry_policy_json {
            Some(json) => match serde_json::from_value::<RetryPolicy>(json.clone()) {
                Ok(policy) => policy,
                Err(e) => {
                    // A malformed policy must not silently kill retries. Fall back to the
                    // default (retrying) policy — matching the long-standing intent of this
                    // branch — rather than sending the task straight to the DLQ.
                    warn!("Failed to parse retry policy: {}. Using default policy.", e);
                    self.default_policy.clone()
                }
            },
            None => {
                debug!("No retry policy found, using default policy");
                self.default_policy.clone()
            }
        };

        // Check if error type is retryable
        if !retry_policy.is_error_retryable(error_type) {
            info!(
                "Error type {:?} is non-retryable, moving to DLQ",
                error_type
            );
            return Ok(RetryDecision::MoveToDlq {
                reason: DlqReason::NonRetryableError {
                    error_type: error_type.unwrap_or("Unknown").to_string(),
                },
            });
        }

        // Check if max attempts exceeded
        if !retry_policy.has_attempts_remaining(current_attempt) {
            info!(
                "Max attempts ({}) exceeded at attempt {}, moving to DLQ",
                retry_policy.max_attempts, current_attempt
            );
            return Ok(RetryDecision::MoveToDlq {
                reason: DlqReason::MaxAttemptsExceeded {
                    max_attempts: retry_policy.max_attempts,
                    current_attempt,
                },
            });
        }

        // Calculate backoff and schedule retry
        let next_attempt = current_attempt + 1;
        let backoff_duration = retry_policy.calculate_backoff(next_attempt);
        let scheduled_at = Timestamptz::now()
            + SignedDuration::try_from(backoff_duration).map_err(|e| {
                OrcherError::internal(format!("Failed to calculate scheduled time: {}", e))
            })?;

        info!(
            "Scheduling retry attempt {} with backoff {:?}",
            next_attempt, backoff_duration
        );

        Ok(RetryDecision::Retry {
            backoff_duration,
            next_attempt,
            scheduled_at,
        })
    }

    /// Evaluate retry decision for a timed-out task
    ///
    /// Timeouts are treated as transient errors and follow the same retry logic
    pub fn evaluate_timeout_retry(
        &self,
        current_attempt: i32,
        retry_policy_json: Option<&serde_json::Value>,
        timeout_type: &str,
    ) -> OrcherResult<RetryDecision> {
        debug!(
            "Evaluating timeout retry for attempt {}, timeout_type: {}",
            current_attempt, timeout_type
        );

        // Timeouts are retryable unless the policy lists the timeout's own
        // failure type (a heartbeat timeout's is `HeartbeatTimeout`), or
        // `TimeoutError`, which has always named every timeout.
        use orcher_storage::schema::types::execution::{
            timeout_failure_type, TIMEOUT_FAILURE_TYPE,
        };
        let own_type = timeout_failure_type(timeout_type);
        if own_type != TIMEOUT_FAILURE_TYPE {
            let decision =
                self.evaluate_retry(current_attempt, retry_policy_json, Some(own_type))?;
            if matches!(
                decision,
                RetryDecision::MoveToDlq {
                    reason: DlqReason::NonRetryableError { .. }
                }
            ) {
                return Ok(decision);
            }
        }
        self.evaluate_retry(
            current_attempt,
            retry_policy_json,
            Some(TIMEOUT_FAILURE_TYPE),
        )
    }

    /// Check if a task should be retried (quick check without full evaluation)
    pub fn should_retry(
        &self,
        current_attempt: i32,
        retry_policy_json: Option<&serde_json::Value>,
        error_type: Option<&str>,
    ) -> bool {
        match self.evaluate_retry(current_attempt, retry_policy_json, error_type) {
            Ok(RetryDecision::Retry { .. }) => true,
            Ok(RetryDecision::MoveToDlq { .. }) => false,
            Err(_) => false,
        }
    }
}

impl Default for RetryPolicyEvaluator {
    fn default() -> Self {
        Self::new()
    }
}

// ================================================================================================
// TESTS
// ================================================================================================

#[cfg(test)]
mod tests {
    use super::*;

    /// A heartbeat timeout is retried unless the policy lists its own type or
    /// `TimeoutError`, which names every timeout; another timeout is not
    /// stopped by listing `HeartbeatTimeout`.
    #[test]
    fn a_heartbeat_timeout_is_retried_unless_its_type_or_every_timeout_is_listed() {
        let evaluator = RetryPolicyEvaluator::new();
        let listing = |types: &[&str]| {
            serde_json::json!({
                "max_attempts": 3,
                "initial_interval_seconds": 1.0,
                "max_interval_seconds": 10.0,
                "backoff_coefficient": 2.0,
                "non_retryable_error_types": types,
            })
        };
        let retried = |policy: &serde_json::Value, kind: &str| {
            matches!(
                evaluator.evaluate_timeout_retry(1, Some(policy), kind),
                Ok(RetryDecision::Retry { .. })
            )
        };
        assert!(retried(&listing(&[]), "Heartbeat"));
        assert!(retried(&listing(&["SomethingElse"]), "Heartbeat"));
        assert!(!retried(&listing(&["HeartbeatTimeout"]), "Heartbeat"));
        assert!(!retried(&listing(&["TimeoutError"]), "Heartbeat"));
        assert!(retried(&listing(&["HeartbeatTimeout"]), "StartToClose"));
        assert!(!retried(&listing(&["TimeoutError"]), "StartToClose"));
    }

    #[test]
    fn test_retry_policy_defaults() {
        let policy = RetryPolicy::default();
        assert_eq!(policy.max_attempts, 3);
        assert_eq!(policy.initial_interval_seconds, 1.0);
        assert_eq!(policy.max_interval_seconds, 3600.0);
        assert_eq!(policy.backoff_coefficient, 2.0);
        assert!(!policy.non_retryable_error_types.is_empty());
    }

    #[test]
    fn test_retry_policy_no_retry() {
        let policy = RetryPolicy::no_retry();
        assert_eq!(policy.max_attempts, 1);
        assert!(!policy.has_attempts_remaining(1));
    }

    #[test]
    fn test_retry_policy_exponential_backoff() {
        let policy = RetryPolicy::exponential_backoff(5, 1.0);
        assert_eq!(policy.max_attempts, 5);
        assert_eq!(policy.initial_interval_seconds, 1.0);
        assert_eq!(policy.backoff_coefficient, 2.0);
    }

    #[test]
    fn test_subsecond_backoff_is_honored() {
        // B10: a sub-second interval (e.g. 250ms) must be preserved, not floored to 0.
        let policy = RetryPolicy::new(3, 0.25, 60.0, 2.0);
        assert_eq!(policy.calculate_backoff(1), Duration::from_millis(250));
        // Exponential: 0.25s * 2^1 = 500ms at attempt 2.
        assert_eq!(policy.calculate_backoff(2), Duration::from_millis(500));

        // A policy deserialized from stored JSON with a fractional interval also works.
        let json = serde_json::json!({
            "max_attempts": 3,
            "initial_interval_seconds": 0.1,
            "max_interval_seconds": 60.0,
            "backoff_coefficient": 2.0,
        });
        let parsed: RetryPolicy = serde_json::from_value(json).unwrap();
        assert_eq!(parsed.calculate_backoff(1), Duration::from_millis(100));
    }

    #[test]
    fn test_backoff_calculation() {
        let policy = RetryPolicy::new(5, 1.0, 60.0, 2.0);

        // Attempt 1: 1s
        assert_eq!(policy.calculate_backoff(1), Duration::from_secs(1));

        // Attempt 2: 1s * 2^1 = 2s
        assert_eq!(policy.calculate_backoff(2), Duration::from_secs(2));

        // Attempt 3: 1s * 2^2 = 4s
        assert_eq!(policy.calculate_backoff(3), Duration::from_secs(4));

        // Attempt 4: 1s * 2^3 = 8s
        assert_eq!(policy.calculate_backoff(4), Duration::from_secs(8));

        // Attempt 5: 1s * 2^4 = 16s
        assert_eq!(policy.calculate_backoff(5), Duration::from_secs(16));

        // Attempt 6: 1s * 2^5 = 32s
        assert_eq!(policy.calculate_backoff(6), Duration::from_secs(32));

        // Attempt 7: 1s * 2^6 = 64s, capped at max_interval (60s)
        assert_eq!(policy.calculate_backoff(7), Duration::from_secs(60));
    }

    #[test]
    fn test_linear_backoff() {
        let policy = RetryPolicy::new(5, 5.0, 100.0, 1.0);

        // With coefficient 1.0, backoff is constant (linear)
        assert_eq!(policy.calculate_backoff(1), Duration::from_secs(5));
        assert_eq!(policy.calculate_backoff(2), Duration::from_secs(5));
        assert_eq!(policy.calculate_backoff(3), Duration::from_secs(5));
    }

    #[test]
    fn test_is_error_retryable() {
        let policy = RetryPolicy::default()
            .with_non_retryable_error("ValidationError")
            .with_non_retryable_error("BadRequestError");

        // Non-retryable errors
        assert!(!policy.is_error_retryable(Some("ValidationError")));
        assert!(!policy.is_error_retryable(Some("BadRequestError")));

        // Retryable errors
        assert!(policy.is_error_retryable(Some("TransientError")));
        assert!(policy.is_error_retryable(Some("TimeoutError")));
        assert!(policy.is_error_retryable(Some("NetworkError")));

        // Unknown errors are retryable
        assert!(policy.is_error_retryable(None));
    }

    #[test]
    fn test_has_attempts_remaining() {
        let policy = RetryPolicy::new(3, 1.0, 60.0, 2.0);

        assert!(policy.has_attempts_remaining(1));
        assert!(policy.has_attempts_remaining(2));
        assert!(!policy.has_attempts_remaining(3));
        assert!(!policy.has_attempts_remaining(4));
    }

    #[test]
    fn test_unlimited_retries() {
        let policy = RetryPolicy::new(0, 1.0, 60.0, 2.0); // 0 = unlimited

        assert!(policy.has_attempts_remaining(1));
        assert!(policy.has_attempts_remaining(100));
        assert!(policy.has_attempts_remaining(1000));
    }

    #[test]
    fn test_evaluator_retry_decision() {
        let evaluator = RetryPolicyEvaluator::new();
        let policy = RetryPolicy::new(3, 1.0, 60.0, 2.0);
        let policy_json = serde_json::to_value(&policy).unwrap();

        // Attempt 1 should retry
        let decision = evaluator
            .evaluate_retry(1, Some(&policy_json), Some("TransientError"))
            .unwrap();
        match decision {
            RetryDecision::Retry {
                next_attempt,
                backoff_duration,
                ..
            } => {
                assert_eq!(next_attempt, 2);
                assert_eq!(backoff_duration, Duration::from_secs(2));
            }
            _ => panic!("Expected Retry decision"),
        }

        // Attempt 2 should retry
        let decision = evaluator
            .evaluate_retry(2, Some(&policy_json), Some("TransientError"))
            .unwrap();
        match decision {
            RetryDecision::Retry {
                next_attempt,
                backoff_duration,
                ..
            } => {
                assert_eq!(next_attempt, 3);
                assert_eq!(backoff_duration, Duration::from_secs(4));
            }
            _ => panic!("Expected Retry decision"),
        }

        // Attempt 3 should move to DLQ (max attempts reached)
        let decision = evaluator
            .evaluate_retry(3, Some(&policy_json), Some("TransientError"))
            .unwrap();
        match decision {
            RetryDecision::MoveToDlq { reason } => match reason {
                DlqReason::MaxAttemptsExceeded {
                    max_attempts,
                    current_attempt,
                } => {
                    assert_eq!(max_attempts, 3);
                    assert_eq!(current_attempt, 3);
                }
                _ => panic!("Expected MaxAttemptsExceeded"),
            },
            _ => panic!("Expected MoveToDlq decision"),
        }
    }

    #[test]
    fn test_evaluator_non_retryable_error() {
        let evaluator = RetryPolicyEvaluator::new();
        let policy = RetryPolicy::default().with_non_retryable_error("ValidationError");
        let policy_json = serde_json::to_value(&policy).unwrap();

        let decision = evaluator
            .evaluate_retry(1, Some(&policy_json), Some("ValidationError"))
            .unwrap();

        match decision {
            RetryDecision::MoveToDlq { reason } => match reason {
                DlqReason::NonRetryableError { error_type } => {
                    assert_eq!(error_type, "ValidationError");
                }
                _ => panic!("Expected NonRetryableError"),
            },
            _ => panic!("Expected MoveToDlq decision"),
        }
    }

    #[test]
    fn test_evaluator_no_retry_policy() {
        let evaluator = RetryPolicyEvaluator::new();

        // When no policy is provided, use default policy
        let decision = evaluator
            .evaluate_retry(1, None, Some("TransientError"))
            .unwrap();

        match decision {
            RetryDecision::Retry { next_attempt, .. } => {
                assert_eq!(next_attempt, 2);
            }
            _ => panic!("Expected Retry decision with default policy"),
        }
    }

    #[test]
    fn test_evaluator_invalid_retry_policy() {
        let evaluator = RetryPolicyEvaluator::new();
        let invalid_json = serde_json::json!({ "invalid": "policy" });

        let decision = evaluator
            .evaluate_retry(1, Some(&invalid_json), Some("TransientError"))
            .unwrap();

        // A malformed policy must not silently kill retries: evaluate_retry falls back to the
        // default (retrying) policy rather than sending the task to the DLQ.
        match decision {
            RetryDecision::Retry { next_attempt, .. } => assert_eq!(next_attempt, 2),
            _ => panic!("Expected Retry decision (invalid policy falls back to default)"),
        }
    }

    #[test]
    fn test_evaluator_timeout_retry() {
        let evaluator = RetryPolicyEvaluator::new();
        let policy = RetryPolicy::new(3, 1.0, 60.0, 2.0);
        let policy_json = serde_json::to_value(&policy).unwrap();

        let decision = evaluator
            .evaluate_timeout_retry(1, Some(&policy_json), "ScheduleToStartTimeout")
            .unwrap();

        match decision {
            RetryDecision::Retry {
                next_attempt,
                backoff_duration,
                ..
            } => {
                assert_eq!(next_attempt, 2);
                assert_eq!(backoff_duration, Duration::from_secs(2));
            }
            _ => panic!("Expected Retry decision for timeout"),
        }
    }

    #[test]
    fn test_should_retry_quick_check() {
        let evaluator = RetryPolicyEvaluator::new();
        let policy = RetryPolicy::new(3, 1.0, 60.0, 2.0);
        let policy_json = serde_json::to_value(&policy).unwrap();

        // Should retry
        assert!(evaluator.should_retry(1, Some(&policy_json), Some("TransientError")));
        assert!(evaluator.should_retry(2, Some(&policy_json), Some("TransientError")));

        // Should not retry (max attempts)
        assert!(!evaluator.should_retry(3, Some(&policy_json), Some("TransientError")));

        // Should not retry (non-retryable error)
        let policy_with_non_retryable =
            RetryPolicy::default().with_non_retryable_error("ValidationError");
        let policy_json = serde_json::to_value(&policy_with_non_retryable).unwrap();
        assert!(!evaluator.should_retry(1, Some(&policy_json), Some("ValidationError")));
    }

    #[test]
    fn test_dlq_reason_display() {
        let reason = DlqReason::MaxAttemptsExceeded {
            max_attempts: 3,
            current_attempt: 3,
        };
        assert_eq!(
            reason.to_string(),
            "Maximum retry attempts exceeded: 3/3 attempts"
        );

        let reason = DlqReason::NonRetryableError {
            error_type: "ValidationError".to_string(),
        };
        assert_eq!(
            reason.to_string(),
            "Non-retryable error type: ValidationError"
        );

        let reason = DlqReason::NoRetryPolicy;
        assert_eq!(reason.to_string(), "No retry policy configured");
    }

    #[test]
    fn test_retry_policy_serialization() {
        let policy =
            RetryPolicy::new(5, 2.0, 300.0, 1.5).with_non_retryable_error("ValidationError");

        // Serialize
        let json = serde_json::to_string(&policy).unwrap();
        assert!(json.contains("max_attempts"));
        assert!(json.contains("ValidationError"));

        // Deserialize
        let deserialized: RetryPolicy = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.max_attempts, 5);
        assert_eq!(deserialized.initial_interval_seconds, 2.0);
        assert_eq!(deserialized.max_interval_seconds, 300.0);
        assert_eq!(deserialized.backoff_coefficient, 1.5);
        assert!(deserialized
            .non_retryable_error_types
            .contains(&"ValidationError".to_string()));
    }

    /// The SDKs call the list `non_retryable_errors`; a policy in their
    /// spelling keeps its list, and is written back in the engine's.
    #[test]
    fn the_sdk_spelling_of_the_non_retryable_list_is_read() {
        let json = serde_json::json!({
            "max_attempts": 5,
            "initial_interval_seconds": 1.0,
            "max_interval_seconds": 60.0,
            "backoff_coefficient": 2.0,
            "non_retryable_errors": ["CardDeclined"],
        });
        let evaluator = RetryPolicyEvaluator::new();
        let decision = evaluator
            .evaluate_retry(1, Some(&json), Some("CardDeclined"))
            .unwrap();
        assert!(
            matches!(decision, RetryDecision::MoveToDlq { .. }),
            "{decision:?}"
        );

        let parsed: RetryPolicy = serde_json::from_value(json).unwrap();
        let written = serde_json::to_value(&parsed).unwrap();
        assert_eq!(written["non_retryable_error_types"][0], "CardDeclined");
        assert!(written.get("non_retryable_errors").is_none());
    }

    /// A policy carrying both names is not a parse error, and both lists
    /// count.
    #[test]
    fn both_spellings_of_the_non_retryable_list_are_merged() {
        let json = serde_json::json!({
            "max_attempts": 5,
            "initial_interval_seconds": 1.0,
            "max_interval_seconds": 60.0,
            "backoff_coefficient": 2.0,
            "non_retryable_error_types": ["CardDeclined"],
            "non_retryable_errors": ["Fraud", "CardDeclined"],
        });
        let parsed: RetryPolicy = serde_json::from_value(json.clone()).unwrap();
        assert_eq!(parsed.non_retryable_error_types, ["CardDeclined", "Fraud"]);
        assert_eq!(non_retryable_error_types(&json), ["CardDeclined", "Fraud"]);

        let evaluator = RetryPolicyEvaluator::new();
        for error_type in ["CardDeclined", "Fraud"] {
            let decision = evaluator
                .evaluate_retry(1, Some(&json), Some(error_type))
                .unwrap();
            assert!(
                matches!(decision, RetryDecision::MoveToDlq { .. }),
                "{error_type}: {decision:?}"
            );
        }
        let decision = evaluator
            .evaluate_retry(1, Some(&json), Some("Timeout"))
            .unwrap();
        assert!(
            matches!(decision, RetryDecision::Retry { .. }),
            "{decision:?}"
        );
    }
}
