//! Retry policy evaluation and decision logic.

use jiff::SignedDuration;
use orcher_common::error::{OrcherError, OrcherResult};
use orcher_common::types::time::Timestamptz;
use orcher_proto::orcher::v1::RetryPolicy;
use orcher_storage::schema::types::execution::{WorkflowExecution, UNLIMITED_RETRIES};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::time::Duration;

// ================================================================================================
// RETRY POLICY
// ================================================================================================

/// Workflow retry policy configuration
///
/// Defines how workflows should be retried when they fail.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowRetryPolicy {
    /// Maximum number of retries, the first run not counted (0 = no retries,
    /// 1 = one retry, etc.): the row's `max_retries`, which a caller's
    /// `maximum_attempts` is converted to by [`max_retries_for_attempts`].
    pub max_attempts: u32,

    /// Initial backoff duration in seconds: the wait before the first retry.
    /// Fractional, because a caller's `initial_interval` can be sub-second.
    pub initial_backoff_secs: f64,

    /// Backoff multiplier for exponential backoff (typically 2.0)
    pub backoff_multiplier: f64,

    /// Maximum backoff duration in seconds (cap on exponential growth)
    pub max_backoff_secs: f64,

    /// List of error types that are retryable
    /// If empty, all errors are considered retryable
    /// If specified, only errors matching these types will trigger retry
    pub non_retryable_error_types: Vec<String>,
}

impl WorkflowRetryPolicy {
    /// Create a default retry policy
    ///
    /// - 3 retry attempts
    /// - 1 second initial backoff
    /// - 2x backoff multiplier
    /// - 300 seconds (5 minutes) max backoff
    /// - All errors are retryable
    pub fn default_policy() -> Self {
        Self {
            max_attempts: 3,
            initial_backoff_secs: 1.0,
            backoff_multiplier: 2.0,
            max_backoff_secs: 300.0,
            non_retryable_error_types: vec![],
        }
    }

    /// Create a policy with no retries
    pub fn no_retry() -> Self {
        Self {
            max_attempts: 0,
            initial_backoff_secs: 0.0,
            backoff_multiplier: 1.0,
            max_backoff_secs: 0.0,
            non_retryable_error_types: vec![],
        }
    }

    /// Create an aggressive retry policy (fast retries, many attempts)
    pub fn aggressive() -> Self {
        Self {
            max_attempts: 10,
            initial_backoff_secs: 1.0,
            backoff_multiplier: 1.5,
            max_backoff_secs: 60.0,
            non_retryable_error_types: vec![],
        }
    }

    /// Create a conservative retry policy (slow retries, few attempts)
    pub fn conservative() -> Self {
        Self {
            max_attempts: 2,
            initial_backoff_secs: 10.0,
            backoff_multiplier: 3.0,
            max_backoff_secs: 600.0,
            non_retryable_error_types: vec![],
        }
    }

    /// The policy a run was started with, read from what its row stores.
    ///
    /// `max_retries` is the row's cap and `stored` its `retry_policy` JSON.
    /// This is the one reading of that JSON. There used to be three, and they
    /// disagreed with what the start path writes: it stores
    /// `initial_backoff_secs` and `max_backoff_secs` as fractional seconds,
    /// the scheduler deserialized them as integers — so every policy with a
    /// duration in it failed to parse and silently became the defaults, its
    /// `non_retryable_error_types` with it — and the other two looked for key
    /// names nothing writes. Both spellings, and whole or fractional seconds,
    /// are accepted here, so rows written by any of them read the same.
    ///
    /// Values that cannot be meant literally are normalised rather than
    /// refused, because a refused policy has no decision to give and a failed
    /// run would be re-evaluated forever: a multiplier below one is taken as
    /// one, and a cap below the initial interval as the initial interval.
    pub fn from_stored(max_retries: i32, stored: Option<&Value>) -> Self {
        let number = |keys: &[&str]| -> Option<f64> {
            let stored = stored?;
            keys.iter()
                .find_map(|key| stored.get(*key).and_then(Value::as_f64))
                .filter(|value| value.is_finite() && *value >= 0.0)
        };

        let initial_backoff_secs =
            number(&["initial_backoff_secs", "initial_interval_secs"]).unwrap_or(1.0);
        let backoff_multiplier = number(&["backoff_multiplier", "backoff_coefficient"])
            .unwrap_or(2.0)
            .max(1.0);
        let max_backoff_secs = number(&["max_backoff_secs", "maximum_interval_secs"])
            .unwrap_or(300.0)
            .max(initial_backoff_secs);
        // Both spellings for the list too: `non_retryable_errors` is the
        // SDKs' name for it.
        let non_retryable_error_types = stored
            .map(crate::retries::retry_policy::non_retryable_error_types)
            .unwrap_or_default();

        Self {
            max_attempts: max_retries.max(0) as u32,
            initial_backoff_secs,
            backoff_multiplier,
            max_backoff_secs,
            non_retryable_error_types,
        }
    }

    /// [`from_stored`](Self::from_stored) for a run's own row.
    pub fn for_execution(execution: &WorkflowExecution) -> Self {
        Self::from_stored(execution.max_retries, execution.retry_policy.as_ref())
    }

    /// How long to wait before retry number `retry` (1 = the first retry).
    ///
    /// The first retry waits the initial interval and each one after that
    /// `backoff_multiplier` times longer, up to the cap — what a caller who
    /// set `initial_interval` means by it.
    pub fn delay_before_retry(&self, retry: u32) -> Duration {
        let exponent = retry.saturating_sub(1).min(i32::MAX as u32) as i32;
        let secs = (self.initial_backoff_secs * self.backoff_multiplier.powi(exponent))
            .min(self.max_backoff_secs);
        Duration::try_from_secs_f64(secs).unwrap_or(Duration::ZERO)
    }

    /// Validate the policy configuration
    pub fn validate(&self) -> OrcherResult<()> {
        if self.backoff_multiplier < 1.0 {
            return Err(OrcherError::validation(
                "backoff_multiplier",
                "backoff_multiplier must be >= 1.0",
            ));
        }

        if self.max_backoff_secs < self.initial_backoff_secs {
            return Err(OrcherError::validation(
                "max_backoff_secs",
                "max_backoff_secs must be >= initial_backoff_secs",
            ));
        }

        Ok(())
    }
}

impl Default for WorkflowRetryPolicy {
    fn default() -> Self {
        Self::default_policy()
    }
}

// ================================================================================================
// RETRY DECISION
// ================================================================================================

/// Decision outcome from retry policy evaluation
#[derive(Debug, Clone, PartialEq)]
pub enum WorkflowRetryDecision {
    /// Workflow should be retried
    Retry {
        /// Duration to wait before retry
        backoff_duration: Duration,
        /// The next attempt number (current + 1)
        next_attempt: u32,
        /// Calculated scheduled time for the retry
        scheduled_at: Timestamptz,
    },
    /// Workflow should not be retried
    Abandon {
        /// Reason for abandoning retry
        reason: AbandonReason,
    },
}

/// Reason why a workflow retry was abandoned
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum AbandonReason {
    /// Maximum retry attempts have been exceeded
    MaxAttemptsExceeded,
    /// Error type is not retryable according to policy
    NonRetryableError {
        /// The error type that was not retryable
        error_type: String,
    },
    /// No retry policy configured
    PolicyDisabled,
    /// The workflow's execution timeout runs out before the retry would
    /// start: the execution timeout bounds every run, so no retry may start
    /// after it.
    ExecutionTimedOut,
    /// The workflow code raised the failure as non-retryable: no retry can
    /// succeed, whatever the policy allows.
    NonRetryableFailure,
}

impl std::fmt::Display for AbandonReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AbandonReason::MaxAttemptsExceeded => {
                write!(f, "Maximum retry attempts exceeded")
            }
            AbandonReason::NonRetryableError { error_type } => {
                write!(f, "Error type '{}' is not retryable", error_type)
            }
            AbandonReason::PolicyDisabled => {
                write!(f, "Retry policy is disabled")
            }
            AbandonReason::ExecutionTimedOut => {
                write!(f, "The workflow execution timeout ends before the retry")
            }
            AbandonReason::NonRetryableFailure => {
                write!(f, "The workflow raised the failure as non-retryable")
            }
        }
    }
}

// ================================================================================================
// RETRY POLICY EVALUATOR
// ================================================================================================

/// Evaluates whether a failed workflow should be retried
///
/// This is a stateless evaluator that makes decisions based on the current
/// attempt number, policy configuration, and error type.
#[derive(Debug, Clone)]
pub struct WorkflowRetryPolicyEvaluator;

impl WorkflowRetryPolicyEvaluator {
    /// Create a new retry policy evaluator
    pub fn new() -> Self {
        Self
    }

    /// Evaluate whether a workflow should be retried
    ///
    /// # Arguments
    ///
    /// * `current_attempt` - The current retry attempt number (0 = first execution, 1 = first retry, etc.)
    /// * `policy` - Optional retry policy. If None, abandons immediately.
    /// * `error_type` - Optional error type classification. Used to check against non_retryable_error_types.
    ///
    /// # Returns
    ///
    /// A `WorkflowRetryDecision` indicating whether to retry or abandon.
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// let evaluator = WorkflowRetryPolicyEvaluator::new();
    /// let policy = WorkflowRetryPolicy::default_policy();
    ///
    /// // First retry (attempt 0 -> attempt 1)
    /// let decision = evaluator.evaluate_retry(0, Some(&policy), None)?;
    /// assert!(matches!(decision, WorkflowRetryDecision::Retry { .. }));
    ///
    /// // After max attempts
    /// let decision = evaluator.evaluate_retry(3, Some(&policy), None)?;
    /// assert!(matches!(decision, WorkflowRetryDecision::Abandon { .. }));
    /// ```
    pub fn evaluate_retry(
        &self,
        current_attempt: u32,
        policy: Option<&WorkflowRetryPolicy>,
        error_type: Option<&str>,
    ) -> OrcherResult<WorkflowRetryDecision> {
        // If no policy provided, abandon immediately
        let policy = match policy {
            Some(p) => p,
            None => {
                return Ok(WorkflowRetryDecision::Abandon {
                    reason: AbandonReason::PolicyDisabled,
                });
            }
        };

        // Validate policy
        policy.validate()?;

        // Check if max attempts exceeded
        let next_attempt = current_attempt + 1;
        if next_attempt > policy.max_attempts {
            return Ok(WorkflowRetryDecision::Abandon {
                reason: AbandonReason::MaxAttemptsExceeded,
            });
        }

        // Check if error type is non-retryable (blocklist semantics).
        // An empty list means all errors are retryable.
        if let Some(error_type) = error_type {
            if policy
                .non_retryable_error_types
                .iter()
                .any(|t| t.eq_ignore_ascii_case(error_type))
            {
                return Ok(WorkflowRetryDecision::Abandon {
                    reason: AbandonReason::NonRetryableError {
                        error_type: error_type.to_string(),
                    },
                });
            }
        }

        // Calculate backoff duration
        let backoff_duration = self.calculate_backoff(next_attempt, policy);

        // Calculate scheduled time
        let scheduled_at = Timestamptz::now()
            + SignedDuration::try_from(backoff_duration).map_err(|e| {
                OrcherError::internal(format!("Failed to convert backoff duration: {}", e))
            })?;

        Ok(WorkflowRetryDecision::Retry {
            backoff_duration,
            next_attempt,
            scheduled_at,
        })
    }

    /// Calculate exponential backoff duration
    ///
    /// Formula: min(initial * (multiplier ^ (attempt - 1)), max_backoff), where
    /// `attempt` is 1-based (1 = first retry). The exponent used to be
    /// `attempt`, so the first retry waited `initial * multiplier` rather than
    /// the initial interval the caller asked for; the delay the engine actually
    /// applied (computed separately at failure time) already started at
    /// `initial`, and this now agrees with it.
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// let evaluator = WorkflowRetryPolicyEvaluator::new();
    /// let policy = WorkflowRetryPolicy {
    ///     initial_backoff_secs: 1.0,
    ///     backoff_multiplier: 2.0,
    ///     max_backoff_secs: 60.0,
    ///     ..Default::default()
    /// };
    ///
    /// assert_eq!(evaluator.calculate_backoff(1, &policy).as_secs(), 1);
    /// assert_eq!(evaluator.calculate_backoff(2, &policy).as_secs(), 2);
    /// assert_eq!(evaluator.calculate_backoff(10, &policy).as_secs(), 60);
    /// ```
    fn calculate_backoff(&self, attempt: u32, policy: &WorkflowRetryPolicy) -> Duration {
        policy.delay_before_retry(attempt)
    }

    /// Whether a failure on attempt `current_attempt` is retried: never when
    /// the workflow code raised it as non-retryable (`non_retryable`), and
    /// otherwise as [`evaluate_retry`](Self::evaluate_retry) decides from the
    /// policy and the failure's type.
    ///
    /// The one place a failure's own mark meets the policy, for the failure
    /// path and the retry scheduler alike, so both reach the same decision.
    pub fn evaluate_failure(
        &self,
        current_attempt: u32,
        policy: Option<&WorkflowRetryPolicy>,
        error_type: Option<&str>,
        non_retryable: bool,
    ) -> OrcherResult<WorkflowRetryDecision> {
        if non_retryable {
            return Ok(WorkflowRetryDecision::Abandon {
                reason: AbandonReason::NonRetryableFailure,
            });
        }
        self.evaluate_retry(current_attempt, policy, error_type)
    }

    /// What to do about a run that has just failed with `failure_type`,
    /// raised as non-retryable when `non_retryable` is set.
    ///
    /// Decided once, when the failure is recorded, from the policy the run was
    /// started with. `Retry` carries when the retry is due; `Abandon` means the
    /// failure is final — the caller settles the run so nothing re-decides it,
    /// and a parent waiting on it can be told.
    pub fn decide_failure(
        &self,
        execution: &WorkflowExecution,
        failure_type: Option<&str>,
        non_retryable: bool,
    ) -> WorkflowRetryDecision {
        self.decide(
            execution.retry_count,
            &WorkflowRetryPolicy::for_execution(execution),
            failure_type,
            non_retryable,
            execution.execution_expires_at(),
        )
    }

    /// What to do about a run that has ended with `failure_type` on attempt
    /// `retry_count`, under `policy`, in a workflow whose execution timeout
    /// runs out at `execution_deadline`. A failure the workflow code raised as
    /// non-retryable (`non_retryable`) is never retried.
    ///
    /// The policy's decision, except that a retry due at or after the
    /// execution deadline is not made: the execution timeout bounds every run
    /// of the workflow, and a retry starting after it would be a run outside
    /// it. The failure stands as the outcome instead.
    pub fn decide(
        &self,
        retry_count: i32,
        policy: &WorkflowRetryPolicy,
        failure_type: Option<&str>,
        non_retryable: bool,
        execution_deadline: Option<Timestamptz>,
    ) -> WorkflowRetryDecision {
        let attempt = retry_count.max(0) as u32;
        let failure_type = failure_type.filter(|t| !t.is_empty());
        // `from_stored` normalises every value `validate` refuses, so this
        // cannot fail; if it ever did, a failure nobody can decide on is final.
        let decision = self
            .evaluate_failure(attempt, Some(policy), failure_type, non_retryable)
            .unwrap_or(WorkflowRetryDecision::Abandon {
                reason: AbandonReason::PolicyDisabled,
            });
        match (&decision, execution_deadline) {
            (WorkflowRetryDecision::Retry { scheduled_at, .. }, Some(deadline))
                if *scheduled_at >= deadline =>
            {
                WorkflowRetryDecision::Abandon {
                    reason: AbandonReason::ExecutionTimedOut,
                }
            }
            _ => decision,
        }
    }
}

/// Whether a recorded workflow failure was raised as non-retryable: the
/// `non_retryable` mark the failure path stores in the run's `error_details`,
/// next to its `failure_type`. A row without the mark was not.
pub fn recorded_non_retryable(error_details: Option<&Value>) -> bool {
    error_details
        .and_then(|details| details.get("non_retryable"))
        .and_then(Value::as_bool)
        .unwrap_or(false)
}

/// The retry count a row stores for a policy's `maximum_attempts`.
///
/// A workflow's `maximum_attempts` counts its runs, the first one included,
/// as a task's does: `N` allows at most `N` runs, so `N - 1` retries, and `1`
/// none. `0` is unlimited, as the proto documents it. The row stores a retry
/// count (`max_retries`, compared against `retry_count`), so the conversion
/// happens here, at the boundary, and nowhere else. It used to store
/// `maximum_attempts` as the retry count, so every policy allowed one run more
/// than it said, and `0` — unlimited — meant no retries at all.
///
/// A negative count is refused: it means nothing, and reading it as some
/// other number would run the workflow a number of times nobody asked for.
pub fn max_retries_for_attempts(maximum_attempts: i32) -> OrcherResult<i32> {
    match maximum_attempts {
        0 => Ok(UNLIMITED_RETRIES),
        n if n > 0 => Ok(n - 1),
        _ => Err(OrcherError::validation(
            "retry_policy.maximum_attempts",
            "maximum_attempts must not be negative (0 is unlimited)",
        )),
    }
}

/// Converts a proto `RetryPolicy` into `(max_retries, retry_policy_json)`, the
/// form a run's row stores. Returns `(None, None)` when no retry policy is
/// provided: a workflow is retried only when its start asks for it.
///
/// Shared by every start path — a top-level start, a batch, a schedule and a
/// child's — so the policy a caller sets means the same thing wherever the run
/// began. `maximum_attempts` is converted by [`max_retries_for_attempts`].
pub fn convert_retry_policy(
    proto: Option<&RetryPolicy>,
) -> OrcherResult<(Option<i32>, Option<Value>)> {
    let Some(rp) = proto else {
        return Ok((None, None));
    };

    let max_retries = Some(max_retries_for_attempts(rp.maximum_attempts)?);

    let initial_interval_secs = rp
        .initial_interval
        .as_ref()
        .map(|d| d.seconds as f64 + d.nanos as f64 / 1_000_000_000.0)
        .unwrap_or(1.0);

    let max_interval_secs = rp
        .maximum_interval
        .as_ref()
        .map(|d| d.seconds as f64 + d.nanos as f64 / 1_000_000_000.0)
        .unwrap_or(300.0);

    let backoff_coefficient = if rp.backoff_coefficient > 0.0 {
        rp.backoff_coefficient
    } else {
        2.0
    };

    let json = serde_json::json!({
        "initial_backoff_secs": initial_interval_secs,
        "backoff_multiplier": backoff_coefficient,
        "max_backoff_secs": max_interval_secs,
        "non_retryable_error_types": rp.non_retryable_error_types,
    });

    Ok((max_retries, Some(json)))
}

impl Default for WorkflowRetryPolicyEvaluator {
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

    #[test]
    fn test_default_policy() {
        let policy = WorkflowRetryPolicy::default_policy();
        assert_eq!(policy.max_attempts, 3);
        assert_eq!(policy.initial_backoff_secs, 1.0);
        assert_eq!(policy.backoff_multiplier, 2.0);
        assert_eq!(policy.max_backoff_secs, 300.0);
        assert!(policy.non_retryable_error_types.is_empty());
    }

    #[test]
    fn test_no_retry_policy() {
        let policy = WorkflowRetryPolicy::no_retry();
        assert_eq!(policy.max_attempts, 0);
    }

    #[test]
    fn test_policy_validation_backoff_multiplier() {
        let policy = WorkflowRetryPolicy {
            backoff_multiplier: 0.5,
            ..Default::default()
        };
        assert!(policy.validate().is_err());
    }

    #[test]
    fn test_policy_validation_max_backoff() {
        let policy = WorkflowRetryPolicy {
            initial_backoff_secs: 100.0,
            max_backoff_secs: 50.0,
            ..Default::default()
        };
        assert!(policy.validate().is_err());
    }

    #[test]
    fn test_evaluate_retry_no_policy() {
        let evaluator = WorkflowRetryPolicyEvaluator::new();
        let decision = evaluator.evaluate_retry(0, None, None).unwrap();

        assert!(matches!(
            decision,
            WorkflowRetryDecision::Abandon {
                reason: AbandonReason::PolicyDisabled
            }
        ));
    }

    #[test]
    fn test_evaluate_retry_within_limits() {
        let evaluator = WorkflowRetryPolicyEvaluator::new();
        let policy = WorkflowRetryPolicy::default_policy();

        let decision = evaluator.evaluate_retry(0, Some(&policy), None).unwrap();

        match decision {
            WorkflowRetryDecision::Retry {
                next_attempt,
                backoff_duration,
                ..
            } => {
                assert_eq!(next_attempt, 1);
                assert_eq!(backoff_duration.as_secs(), 1); // the initial interval
            }
            _ => panic!("Expected Retry decision"),
        }
    }

    #[test]
    fn test_evaluate_retry_max_attempts_exceeded() {
        let evaluator = WorkflowRetryPolicyEvaluator::new();
        let policy = WorkflowRetryPolicy::default_policy(); // max_attempts = 3

        // Attempt 3 means we've already tried 4 times (0, 1, 2, 3)
        let decision = evaluator.evaluate_retry(3, Some(&policy), None).unwrap();

        assert!(matches!(
            decision,
            WorkflowRetryDecision::Abandon {
                reason: AbandonReason::MaxAttemptsExceeded
            }
        ));
    }

    #[test]
    fn test_evaluate_retry_non_retryable_error() {
        let evaluator = WorkflowRetryPolicyEvaluator::new();
        // non_retryable_error_types is a blocklist: listed types → Abandon
        let policy = WorkflowRetryPolicy {
            non_retryable_error_types: vec![
                "TransientError".to_string(),
                "TimeoutError".to_string(),
            ],
            ..Default::default()
        };

        let decision = evaluator
            .evaluate_retry(0, Some(&policy), Some("TransientError"))
            .unwrap();

        match decision {
            WorkflowRetryDecision::Abandon {
                reason: AbandonReason::NonRetryableError { error_type },
            } => {
                assert_eq!(error_type, "TransientError");
            }
            _ => panic!("Expected Abandon decision"),
        }
    }

    #[test]
    fn test_evaluate_retry_retryable_error() {
        let evaluator = WorkflowRetryPolicyEvaluator::new();
        // non_retryable_error_types is a blocklist: unlisted types → Retry
        let policy = WorkflowRetryPolicy {
            non_retryable_error_types: vec![
                "TransientError".to_string(),
                "TimeoutError".to_string(),
            ],
            ..Default::default()
        };

        let decision = evaluator
            .evaluate_retry(0, Some(&policy), Some("PermanentError"))
            .unwrap();

        assert!(matches!(decision, WorkflowRetryDecision::Retry { .. }));
    }

    /// A failure the workflow code raised as non-retryable is not retried,
    /// with retries left and a type the policy does not name; the same
    /// failure unmarked is.
    #[test]
    fn a_failure_raised_as_non_retryable_is_not_retried() {
        let evaluator = WorkflowRetryPolicyEvaluator::new();
        let policy = WorkflowRetryPolicy {
            max_attempts: 3,
            ..Default::default()
        };
        assert_eq!(
            evaluator.decide(0, &policy, Some("OutOfStock"), true, None),
            WorkflowRetryDecision::Abandon {
                reason: AbandonReason::NonRetryableFailure
            }
        );
        assert!(matches!(
            evaluator.decide(0, &policy, Some("OutOfStock"), false, None),
            WorkflowRetryDecision::Retry { .. }
        ));
    }

    /// The mark is read from what the failure path stores; a row without it
    /// was not marked.
    #[test]
    fn the_recorded_mark_is_read_from_the_failure_details() {
        let marked = serde_json::json!({"failure_type": "OutOfStock", "non_retryable": true});
        let unmarked = serde_json::json!({"failure_type": "OutOfStock", "non_retryable": false});
        let older = serde_json::json!({"failure_type": "OutOfStock"});
        assert!(recorded_non_retryable(Some(&marked)));
        assert!(!recorded_non_retryable(Some(&unmarked)));
        assert!(!recorded_non_retryable(Some(&older)));
        assert!(!recorded_non_retryable(None));
    }

    /// No retry is scheduled that would start at or past the execution
    /// deadline; one due before it is.
    #[test]
    fn no_retry_starts_past_the_execution_deadline() {
        let evaluator = WorkflowRetryPolicyEvaluator::new();
        let policy = WorkflowRetryPolicy {
            max_attempts: 3,
            initial_backoff_secs: 10.0,
            ..Default::default()
        };
        let in_5s = Timestamptz::now() + Duration::from_secs(5);
        assert_eq!(
            evaluator.decide(0, &policy, None, false, Some(in_5s)),
            WorkflowRetryDecision::Abandon {
                reason: AbandonReason::ExecutionTimedOut
            }
        );
        let in_an_hour = Timestamptz::now() + Duration::from_secs(3600);
        assert!(matches!(
            evaluator.decide(0, &policy, None, false, Some(in_an_hour)),
            WorkflowRetryDecision::Retry { .. }
        ));
        assert!(matches!(
            evaluator.decide(0, &policy, None, false, None),
            WorkflowRetryDecision::Retry { .. }
        ));
    }

    #[test]
    fn test_calculate_backoff_exponential() {
        let evaluator = WorkflowRetryPolicyEvaluator::new();
        let policy = WorkflowRetryPolicy {
            initial_backoff_secs: 1.0,
            backoff_multiplier: 2.0,
            max_backoff_secs: 1000.0,
            ..Default::default()
        };

        // Retry 1 waits the initial interval; each later one doubles it.
        let backoff = evaluator.calculate_backoff(1, &policy);
        assert_eq!(backoff.as_secs(), 1);

        let backoff = evaluator.calculate_backoff(2, &policy);
        assert_eq!(backoff.as_secs(), 2);

        let backoff = evaluator.calculate_backoff(3, &policy);
        assert_eq!(backoff.as_secs(), 4);

        let backoff = evaluator.calculate_backoff(4, &policy);
        assert_eq!(backoff.as_secs(), 8);
    }

    #[test]
    fn test_calculate_backoff_capped() {
        let evaluator = WorkflowRetryPolicyEvaluator::new();
        let policy = WorkflowRetryPolicy {
            initial_backoff_secs: 1.0,
            backoff_multiplier: 2.0,
            max_backoff_secs: 10.0,
            ..Default::default()
        };

        // Retry 10: 1 * (2^9) = 512, but capped at 10
        let backoff = evaluator.calculate_backoff(10, &policy);
        assert_eq!(backoff.as_secs(), 10);
    }

    #[test]
    fn test_calculate_backoff_different_multiplier() {
        let evaluator = WorkflowRetryPolicyEvaluator::new();
        let policy = WorkflowRetryPolicy {
            initial_backoff_secs: 5.0,
            backoff_multiplier: 1.5,
            max_backoff_secs: 1000.0,
            ..Default::default()
        };

        // Retry 2: 5 * 1.5 = 7.5
        let backoff = evaluator.calculate_backoff(2, &policy);
        assert_eq!(backoff.as_millis(), 7_500);

        // Retry 3: 5 * 1.5^2 = 11.25
        let backoff = evaluator.calculate_backoff(3, &policy);
        assert_eq!(backoff.as_millis(), 11_250);
    }

    #[test]
    fn test_abandon_reason_display() {
        assert_eq!(
            AbandonReason::MaxAttemptsExceeded.to_string(),
            "Maximum retry attempts exceeded"
        );

        assert_eq!(
            AbandonReason::NonRetryableError {
                error_type: "ValidationError".to_string()
            }
            .to_string(),
            "Error type 'ValidationError' is not retryable"
        );

        assert_eq!(
            AbandonReason::PolicyDisabled.to_string(),
            "Retry policy is disabled"
        );
    }

    #[test]
    fn test_aggressive_policy() {
        let policy = WorkflowRetryPolicy::aggressive();
        assert_eq!(policy.max_attempts, 10);
        assert_eq!(policy.initial_backoff_secs, 1.0);
        assert_eq!(policy.backoff_multiplier, 1.5);
        assert_eq!(policy.max_backoff_secs, 60.0);
    }

    #[test]
    fn test_conservative_policy() {
        let policy = WorkflowRetryPolicy::conservative();
        assert_eq!(policy.max_attempts, 2);
        assert_eq!(policy.initial_backoff_secs, 10.0);
        assert_eq!(policy.backoff_multiplier, 3.0);
        assert_eq!(policy.max_backoff_secs, 600.0);
    }

    #[test]
    fn test_retry_decision_equality() {
        let now = Timestamptz::now();
        let decision1 = WorkflowRetryDecision::Retry {
            backoff_duration: Duration::from_secs(5),
            next_attempt: 2,
            scheduled_at: now,
        };
        let decision2 = WorkflowRetryDecision::Retry {
            backoff_duration: Duration::from_secs(5),
            next_attempt: 2,
            scheduled_at: now,
        };
        assert_eq!(decision1, decision2);
    }

    /// What the start path stores: fractional seconds under the current
    /// names. The scheduler used to read these as integers, fail, and fall
    /// back to defaults — dropping the non-retryable list with them.
    #[test]
    fn a_stored_policy_with_fractional_seconds_is_read_as_written() {
        let stored = serde_json::json!({
            "initial_backoff_secs": 0.5,
            "backoff_multiplier": 3.0,
            "max_backoff_secs": 12.5,
            "non_retryable_error_types": ["ValidationError"],
        });
        let policy = WorkflowRetryPolicy::from_stored(4, Some(&stored));
        assert_eq!(policy.max_attempts, 4);
        assert_eq!(policy.initial_backoff_secs, 0.5);
        assert_eq!(policy.backoff_multiplier, 3.0);
        assert_eq!(policy.max_backoff_secs, 12.5);
        assert_eq!(policy.non_retryable_error_types, vec!["ValidationError"]);
        assert_eq!(policy.delay_before_retry(1).as_millis(), 500);
        assert_eq!(policy.delay_before_retry(2).as_millis(), 1_500);
        assert_eq!(policy.delay_before_retry(9).as_millis(), 12_500);
    }

    #[test]
    fn whole_seconds_read_the_same() {
        let stored = serde_json::json!({
            "initial_backoff_secs": 2,
            "backoff_multiplier": 2,
            "max_backoff_secs": 60,
        });
        let policy = WorkflowRetryPolicy::from_stored(1, Some(&stored));
        assert_eq!(policy.initial_backoff_secs, 2.0);
        assert_eq!(policy.backoff_multiplier, 2.0);
        assert_eq!(policy.max_backoff_secs, 60.0);
        assert!(policy.non_retryable_error_types.is_empty());
    }

    /// The names the two failure-time delay functions looked for.
    #[test]
    fn the_legacy_key_names_are_accepted() {
        let stored = serde_json::json!({
            "initial_interval_secs": 3,
            "backoff_coefficient": 1.5,
            "maximum_interval_secs": 30.0,
        });
        let policy = WorkflowRetryPolicy::from_stored(2, Some(&stored));
        assert_eq!(policy.initial_backoff_secs, 3.0);
        assert_eq!(policy.backoff_multiplier, 1.5);
        assert_eq!(policy.max_backoff_secs, 30.0);
    }

    #[test]
    fn no_stored_policy_means_the_defaults_and_the_rows_cap() {
        let policy = WorkflowRetryPolicy::from_stored(0, None);
        assert_eq!(policy.max_attempts, 0);
        assert_eq!(policy.initial_backoff_secs, 1.0);
        assert_eq!(policy.backoff_multiplier, 2.0);
        assert_eq!(policy.max_backoff_secs, 300.0);
    }

    /// A policy `validate` would refuse has no decision to give, and a failed
    /// run whose policy cannot decide is re-evaluated forever; it is
    /// normalised instead.
    #[test]
    fn values_that_cannot_be_meant_literally_are_normalised() {
        let stored = serde_json::json!({
            "initial_backoff_secs": 10,
            "backoff_multiplier": 0.5,
            "max_backoff_secs": 1,
        });
        let policy = WorkflowRetryPolicy::from_stored(3, Some(&stored));
        assert_eq!(policy.backoff_multiplier, 1.0);
        assert_eq!(policy.max_backoff_secs, 10.0);
        assert!(policy.validate().is_ok());
    }

    /// The conversion every start path stores, read back by the one parser.
    #[test]
    fn a_converted_proto_policy_round_trips() {
        let proto = RetryPolicy {
            initial_interval: Some(prost_types::Duration {
                seconds: 0,
                nanos: 250_000_000,
            }),
            backoff_coefficient: 2.0,
            maximum_interval: Some(prost_types::Duration {
                seconds: 4,
                nanos: 0,
            }),
            maximum_attempts: 3,
            non_retryable_error_types: vec!["Fatal".to_string()],
        };
        let (max_retries, stored) = convert_retry_policy(Some(&proto)).unwrap();
        // Three attempts, the first included: two retries.
        assert_eq!(max_retries, Some(2));
        let policy = WorkflowRetryPolicy::from_stored(max_retries.unwrap(), stored.as_ref());
        assert_eq!(policy.max_attempts, 2);
        assert_eq!(policy.delay_before_retry(1).as_millis(), 250);
        assert_eq!(policy.max_backoff_secs, 4.0);
        assert_eq!(policy.non_retryable_error_types, vec!["Fatal"]);
    }

    /// `maximum_attempts` counts runs, the first included; 0 is unlimited.
    #[test]
    fn maximum_attempts_counts_the_first_run() {
        assert_eq!(max_retries_for_attempts(1).unwrap(), 0);
        assert_eq!(max_retries_for_attempts(2).unwrap(), 1);
        assert_eq!(max_retries_for_attempts(5).unwrap(), 4);
        assert_eq!(max_retries_for_attempts(0).unwrap(), UNLIMITED_RETRIES);
        assert!(max_retries_for_attempts(-1).is_err());
        assert_eq!(convert_retry_policy(None).unwrap(), (None, None));
    }

    /// An unlimited policy keeps retrying, and its backoff stays bounded.
    #[test]
    fn an_unlimited_policy_never_runs_out() {
        let policy = WorkflowRetryPolicy::from_stored(UNLIMITED_RETRIES, None);
        let decision = WorkflowRetryPolicyEvaluator::new()
            .evaluate_retry(1_000_000, Some(&policy), None)
            .unwrap();
        assert!(matches!(decision, WorkflowRetryDecision::Retry { .. }));
        assert!(policy.delay_before_retry(1_000_000).as_secs_f64() <= policy.max_backoff_secs);
    }
}
