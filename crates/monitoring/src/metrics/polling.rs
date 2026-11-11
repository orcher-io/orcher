//! Polling performance metrics (latency, Redis/Postgres timing, capacity).

use metrics::{counter, describe_counter, describe_gauge, describe_histogram, gauge, histogram};
use std::future::Future;
use std::time::{Duration, Instant};
use tracing::{debug, warn};

pub mod names {
    pub const POLL_DURATION_SECONDS: &str = "orcher_poll_duration_seconds";
    pub const POLL_TOTAL: &str = "orcher_poll_total";
    pub const POLL_SUCCESS_TOTAL: &str = "orcher_poll_success_total";
    pub const POLL_TIMEOUT_TOTAL: &str = "orcher_poll_timeout_total";
    pub const POLL_ERROR_TOTAL: &str = "orcher_poll_error_total";
    pub const REDIS_PUSH_DURATION_SECONDS: &str = "orcher_redis_push_duration_seconds";
    pub const REDIS_POP_DURATION_SECONDS: &str = "orcher_redis_pop_duration_seconds";
    pub const CLAIM_DURATION_SECONDS: &str = "orcher_claim_duration_seconds";
    pub const POLL_QUEUE_DEPTH: &str = "orcher_poll_queue_depth";
    pub const PUSH_TO_CLAIM_LATENCY_SECONDS: &str = "orcher_push_to_claim_latency_seconds";
    pub const REDIS_RETRY_TOTAL: &str = "orcher_redis_retry_total";
    pub const POSTGRES_FALLBACK_TOTAL: &str = "orcher_postgres_fallback_total";
    pub const BATCH_CLAIM_SIZE: &str = "orcher_batch_claim_size";
    pub const BATCH_CLAIM_SUCCESS_RATIO: &str = "orcher_batch_claim_success_ratio";
    /// Time from task scheduled to service pickup.
    pub const SCHEDULE_TO_START_SECONDS: &str = "orcher_schedule_to_start_seconds";
    pub const WORKER_TASK_PERMITS_AVAILABLE: &str = "orcher_worker_task_permits_available";
    pub const WORKER_TASK_PERMITS_USED: &str = "orcher_worker_task_permits_used";
    pub const WORKER_TASK_PERMITS_TOTAL: &str = "orcher_worker_task_permits_total";
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PollOutcome {
    Success,
    Timeout,
    Error,
}

impl PollOutcome {
    pub fn as_str(&self) -> &'static str {
        match self {
            PollOutcome::Success => "success",
            PollOutcome::Timeout => "timeout",
            PollOutcome::Error => "error",
        }
    }
}

/// Register metric descriptions. Call once at startup.
pub fn describe_polling_metrics() {
    describe_histogram!(
        names::POLL_DURATION_SECONDS,
        "End-to-end poll request duration in seconds"
    );
    describe_counter!(names::POLL_TOTAL, "Total number of poll requests");
    describe_counter!(
        names::POLL_SUCCESS_TOTAL,
        "Number of polls that returned work successfully"
    );
    describe_counter!(
        names::POLL_TIMEOUT_TOTAL,
        "Number of polls that timed out waiting for work"
    );
    describe_counter!(
        names::POLL_ERROR_TOTAL,
        "Number of polls that failed with an error"
    );
    describe_histogram!(
        names::REDIS_PUSH_DURATION_SECONDS,
        "Redis LPUSH latency in seconds (workflow dispatch to Redis)"
    );
    describe_histogram!(
        names::REDIS_POP_DURATION_SECONDS,
        "Redis BRPOP latency in seconds (waiting for work)"
    );
    describe_histogram!(
        names::CLAIM_DURATION_SECONDS,
        "PostgreSQL claim operation latency in seconds"
    );
    describe_gauge!(
        names::POLL_QUEUE_DEPTH,
        "Current number of pending/active polls per queue"
    );
    describe_histogram!(
        names::PUSH_TO_CLAIM_LATENCY_SECONDS,
        "Time from LPUSH to successful claim (end-to-end matching latency)"
    );
    describe_counter!(
        names::REDIS_RETRY_TOTAL,
        "Number of Redis retries before fallback to PostgreSQL"
    );
    describe_counter!(
        names::POSTGRES_FALLBACK_TOTAL,
        "Number of fallbacks to PostgreSQL polling"
    );
    describe_histogram!(
        names::BATCH_CLAIM_SIZE,
        "Number of execution IDs in batch claim operations"
    );
    describe_gauge!(
        names::BATCH_CLAIM_SUCCESS_RATIO,
        "Ratio of successful claims in batch operations"
    );
    describe_histogram!(
        names::SCHEDULE_TO_START_SECONDS,
        "Time from task scheduled to service pickup"
    );
    describe_gauge!(
        names::WORKER_TASK_PERMITS_AVAILABLE,
        "Available worker task permits (remaining capacity)"
    );
    describe_gauge!(
        names::WORKER_TASK_PERMITS_USED,
        "Used worker task permits (current load)"
    );
    describe_gauge!(
        names::WORKER_TASK_PERMITS_TOTAL,
        "Total worker task permits capacity"
    );
}

pub fn record_poll_latency(
    task_queue: &str,
    namespace: &str,
    duration: Duration,
    outcome: PollOutcome,
) {
    histogram!(
        names::POLL_DURATION_SECONDS,
        "task_queue" => task_queue.to_string(),
        "namespace" => namespace.to_string(),
        "outcome" => outcome.as_str().to_string()
    )
    .record(duration.as_secs_f64());

    counter!(
        names::POLL_TOTAL,
        "task_queue" => task_queue.to_string(),
        "namespace" => namespace.to_string()
    )
    .increment(1);

    match outcome {
        PollOutcome::Success => {
            counter!(
                names::POLL_SUCCESS_TOTAL,
                "task_queue" => task_queue.to_string(),
                "namespace" => namespace.to_string()
            )
            .increment(1);
        }
        PollOutcome::Timeout => {
            counter!(
                names::POLL_TIMEOUT_TOTAL,
                "task_queue" => task_queue.to_string(),
                "namespace" => namespace.to_string()
            )
            .increment(1);
        }
        PollOutcome::Error => {
            counter!(
                names::POLL_ERROR_TOTAL,
                "task_queue" => task_queue.to_string(),
                "namespace" => namespace.to_string()
            )
            .increment(1);
        }
    }

    // Log slow polls
    if duration.as_secs() > 5 {
        warn!(
            task_queue = %task_queue,
            namespace = %namespace,
            duration_secs = duration.as_secs_f64(),
            outcome = outcome.as_str(),
            "Slow poll detected"
        );
    }
}

pub fn record_redis_push_latency(task_queue: &str, duration: Duration) {
    histogram!(
        names::REDIS_PUSH_DURATION_SECONDS,
        "task_queue" => task_queue.to_string()
    )
    .record(duration.as_secs_f64());

    debug!(
        task_queue = %task_queue,
        duration_ms = duration.as_millis(),
        "Redis LPUSH completed"
    );
}

pub fn record_redis_pop_latency(task_queue: &str, duration: Duration, got_work: bool) {
    histogram!(
        names::REDIS_POP_DURATION_SECONDS,
        "task_queue" => task_queue.to_string(),
        "got_work" => got_work.to_string()
    )
    .record(duration.as_secs_f64());

    debug!(
        task_queue = %task_queue,
        duration_ms = duration.as_millis(),
        got_work = got_work,
        "Redis BRPOP completed"
    );
}

pub fn record_claim_latency(task_queue: &str, duration: Duration, success: bool) {
    histogram!(
        names::CLAIM_DURATION_SECONDS,
        "task_queue" => task_queue.to_string(),
        "success" => success.to_string()
    )
    .record(duration.as_secs_f64());

    if duration.as_millis() > 100 {
        warn!(
            task_queue = %task_queue,
            duration_ms = duration.as_millis(),
            success = success,
            "Slow claim operation detected"
        );
    }
}

/// Time from Redis LPUSH to successful claim by a poller.
pub fn record_push_to_claim_latency(task_queue: &str, duration: Duration) {
    histogram!(
        names::PUSH_TO_CLAIM_LATENCY_SECONDS,
        "task_queue" => task_queue.to_string()
    )
    .record(duration.as_secs_f64());

    if duration.as_secs() > 3 {
        warn!(
            task_queue = %task_queue,
            duration_secs = duration.as_secs_f64(),
            "High push-to-claim latency detected"
        );
    }
}

pub fn update_poll_queue_depth(task_queue: &str, namespace: &str, depth: u64) {
    gauge!(
        names::POLL_QUEUE_DEPTH,
        "task_queue" => task_queue.to_string(),
        "namespace" => namespace.to_string()
    )
    .set(depth as f64);
}

pub fn increment_poll_queue_depth(task_queue: &str, namespace: &str) {
    gauge!(
        names::POLL_QUEUE_DEPTH,
        "task_queue" => task_queue.to_string(),
        "namespace" => namespace.to_string()
    )
    .set(1.0);
}

pub fn decrement_poll_queue_depth(task_queue: &str, namespace: &str) {
    gauge!(
        names::POLL_QUEUE_DEPTH,
        "task_queue" => task_queue.to_string(),
        "namespace" => namespace.to_string()
    )
    .set(-1.0);
}

pub fn record_redis_retry(task_queue: &str, retry_count: u32) {
    counter!(
        names::REDIS_RETRY_TOTAL,
        "task_queue" => task_queue.to_string(),
        "retry_count" => retry_count.to_string()
    )
    .increment(1);
}

pub fn record_postgres_fallback(task_queue: &str, reason: &str) {
    counter!(
        names::POSTGRES_FALLBACK_TOTAL,
        "task_queue" => task_queue.to_string(),
        "reason" => reason.to_string()
    )
    .increment(1);
}

pub fn record_batch_claim(task_queue: &str, requested: usize, claimed: usize) {
    histogram!(
        names::BATCH_CLAIM_SIZE,
        "task_queue" => task_queue.to_string()
    )
    .record(requested as f64);

    let success_ratio = if requested > 0 {
        claimed as f64 / requested as f64
    } else {
        1.0
    };

    gauge!(
        names::BATCH_CLAIM_SUCCESS_RATIO,
        "task_queue" => task_queue.to_string()
    )
    .set(success_ratio);

    if success_ratio < 0.5 && requested > 0 {
        warn!(
            task_queue = %task_queue,
            requested = requested,
            claimed = claimed,
            success_ratio = %format!("{:.2}", success_ratio),
            "Low batch claim success ratio"
        );
    }
}

/// Time from submitted_at to claimed_at. Target: <100ms P99.
pub fn record_schedule_to_start_latency(
    task_queue: &str,
    namespace: &str,
    latency: std::time::Duration,
) {
    histogram!(
        names::SCHEDULE_TO_START_SECONDS,
        "task_queue" => task_queue.to_string(),
        "namespace" => namespace.to_string()
    )
    .record(latency.as_secs_f64());
}

pub fn update_worker_capacity(
    task_queue: &str,
    namespace: &str,
    worker_id: &str,
    available: usize,
    used: usize,
    total: usize,
) {
    gauge!(
        names::WORKER_TASK_PERMITS_AVAILABLE,
        "task_queue" => task_queue.to_string(),
        "namespace" => namespace.to_string(),
        "worker_id" => worker_id.to_string()
    )
    .set(available as f64);
    gauge!(
        names::WORKER_TASK_PERMITS_USED,
        "task_queue" => task_queue.to_string(),
        "namespace" => namespace.to_string(),
        "worker_id" => worker_id.to_string()
    )
    .set(used as f64);
    gauge!(
        names::WORKER_TASK_PERMITS_TOTAL,
        "task_queue" => task_queue.to_string(),
        "namespace" => namespace.to_string(),
        "worker_id" => worker_id.to_string()
    )
    .set(total as f64);
}

/// Wraps a poll future with automatic timing and outcome recording.
pub async fn track_poll_operation<F, T, E>(
    task_queue: &str,
    namespace: &str,
    f: F,
) -> Result<Option<T>, E>
where
    F: Future<Output = Result<Option<T>, E>>,
{
    let start = Instant::now();

    // Track that a poll is in progress
    increment_poll_queue_depth(task_queue, namespace);

    let result = f.await;
    let duration = start.elapsed();

    // Track poll completion
    decrement_poll_queue_depth(task_queue, namespace);

    // Determine outcome and record metrics
    let outcome = match &result {
        Ok(Some(_)) => PollOutcome::Success,
        Ok(None) => PollOutcome::Timeout,
        Err(_) => PollOutcome::Error,
    };

    record_poll_latency(task_queue, namespace, duration, outcome);

    result
}

/// RAII guard that records poll duration and outcome on drop.
pub struct PollDurationGuard {
    task_queue: String,
    namespace: String,
    start: Instant,
    outcome: Option<PollOutcome>,
}

impl PollDurationGuard {
    pub fn new(task_queue: &str, namespace: &str) -> Self {
        increment_poll_queue_depth(task_queue, namespace);

        Self {
            task_queue: task_queue.to_string(),
            namespace: namespace.to_string(),
            start: Instant::now(),
            outcome: None,
        }
    }

    pub fn set_outcome(&mut self, outcome: PollOutcome) {
        self.outcome = Some(outcome);
    }

    pub fn success(&mut self) {
        self.outcome = Some(PollOutcome::Success);
    }

    pub fn timeout(&mut self) {
        self.outcome = Some(PollOutcome::Timeout);
    }

    pub fn error(&mut self) {
        self.outcome = Some(PollOutcome::Error);
    }

    pub fn elapsed(&self) -> Duration {
        self.start.elapsed()
    }
}

impl Drop for PollDurationGuard {
    fn drop(&mut self) {
        let duration = self.start.elapsed();
        let outcome = self.outcome.unwrap_or(PollOutcome::Error);

        decrement_poll_queue_depth(&self.task_queue, &self.namespace);
        record_poll_latency(&self.task_queue, &self.namespace, duration, outcome);
    }
}

/// RAII guard that records Redis BRPOP latency on drop.
pub struct RedisPopGuard {
    task_queue: String,
    start: Instant,
    got_work: bool,
}

impl RedisPopGuard {
    pub fn new(task_queue: &str) -> Self {
        Self {
            task_queue: task_queue.to_string(),
            start: Instant::now(),
            got_work: false,
        }
    }

    pub fn received_work(&mut self) {
        self.got_work = true;
    }

    pub fn elapsed(&self) -> Duration {
        self.start.elapsed()
    }
}

impl Drop for RedisPopGuard {
    fn drop(&mut self) {
        record_redis_pop_latency(&self.task_queue, self.start.elapsed(), self.got_work);
    }
}

/// RAII guard that records claim latency on drop.
pub struct ClaimGuard {
    task_queue: String,
    start: Instant,
    success: bool,
}

impl ClaimGuard {
    pub fn new(task_queue: &str) -> Self {
        Self {
            task_queue: task_queue.to_string(),
            start: Instant::now(),
            success: false,
        }
    }

    pub fn success(&mut self) {
        self.success = true;
    }

    pub fn elapsed(&self) -> Duration {
        self.start.elapsed()
    }
}

impl Drop for ClaimGuard {
    fn drop(&mut self) {
        record_claim_latency(&self.task_queue, self.start.elapsed(), self.success);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_poll_outcome_as_str() {
        assert_eq!(PollOutcome::Success.as_str(), "success");
        assert_eq!(PollOutcome::Timeout.as_str(), "timeout");
        assert_eq!(PollOutcome::Error.as_str(), "error");
    }

    #[test]
    fn test_describe_polling_metrics() {
        // Should not panic
        describe_polling_metrics();
    }

    #[test]
    fn test_record_poll_latency() {
        // Should not panic
        record_poll_latency(
            "test-queue",
            "test-ns",
            Duration::from_millis(100),
            PollOutcome::Success,
        );
    }

    #[test]
    fn test_record_redis_push_latency() {
        record_redis_push_latency("test-queue", Duration::from_millis(5));
    }

    #[test]
    fn test_record_redis_pop_latency() {
        record_redis_pop_latency("test-queue", Duration::from_millis(1000), true);
        record_redis_pop_latency("test-queue", Duration::from_millis(5000), false);
    }

    #[test]
    fn test_record_claim_latency() {
        record_claim_latency("test-queue", Duration::from_millis(15), true);
        record_claim_latency("test-queue", Duration::from_millis(150), false);
    }

    #[test]
    fn test_record_push_to_claim_latency() {
        record_push_to_claim_latency("test-queue", Duration::from_millis(100));
        record_push_to_claim_latency("test-queue", Duration::from_secs(5));
    }

    #[test]
    fn test_record_batch_claim() {
        record_batch_claim("test-queue", 5, 5);
        record_batch_claim("test-queue", 5, 2);
        record_batch_claim("test-queue", 0, 0);
    }

    #[test]
    fn test_poll_duration_guard() {
        let mut guard = PollDurationGuard::new("test-queue", "test-ns");
        guard.success();
        assert!(guard.elapsed() >= Duration::ZERO);
        // Guard will record metrics on drop
    }

    #[test]
    fn test_redis_pop_guard() {
        let mut guard = RedisPopGuard::new("test-queue");
        guard.received_work();
        assert!(guard.elapsed() >= Duration::ZERO);
    }

    #[test]
    fn test_claim_guard() {
        let mut guard = ClaimGuard::new("test-queue");
        guard.success();
        assert!(guard.elapsed() >= Duration::ZERO);
    }

    #[tokio::test]
    async fn test_track_poll_operation_success() {
        let result: Result<Option<String>, &str> =
            track_poll_operation("test-queue", "test-ns", async {
                Ok(Some("work".to_string()))
            })
            .await;

        assert!(result.is_ok());
        assert!(result.unwrap().is_some());
    }

    #[tokio::test]
    async fn test_track_poll_operation_timeout() {
        let result: Result<Option<String>, &str> =
            track_poll_operation("test-queue", "test-ns", async { Ok(None) }).await;

        assert!(result.is_ok());
        assert!(result.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_track_poll_operation_error() {
        let result: Result<Option<String>, &str> =
            track_poll_operation("test-queue", "test-ns", async { Err("test error") }).await;

        assert!(result.is_err());
    }
}
