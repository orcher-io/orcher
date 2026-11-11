//! Timer queue processor for scheduled events.

use crate::engine::queues::{TimerQueue, TimerTask};
use orcher_common::error::OrcherResult;
use orcher_common::types::time::Timestamptz;
use orcher_storage::repositories::domains::coordination::CoordinationRepository;
use orcher_storage::traits::domain::CoordinationRepositoryTrait;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, Notify};
use tokio::time::sleep;
use tracing::{debug, error, info, warn};

/// Timer processor configuration
#[derive(Debug, Clone)]
pub struct TimerProcessorConfig {
    /// Number of timers to claim per batch
    pub batch_size: usize,

    /// Maximum sleep duration between checks.
    ///
    /// This is a backstop, not the primary wake-up: a timer scheduled by *this*
    /// process signals `wake_handle()` and is picked up immediately. The bound
    /// matters when another node schedules the timer, since a `Notify` does not
    /// cross process boundaries.
    pub max_sleep_duration: Duration,

    /// Minimum sleep duration (prevents tight loops)
    pub min_sleep_duration: Duration,

    /// Threshold for stuck timer detection (seconds)
    pub stuck_threshold_secs: i64,

    /// Cleanup interval
    pub cleanup_interval: Duration,
}

impl Default for TimerProcessorConfig {
    fn default() -> Self {
        Self {
            batch_size: 100,
            max_sleep_duration: Duration::from_secs(1),
            min_sleep_duration: Duration::from_millis(10),
            stuck_threshold_secs: 300, // 5 minutes
            cleanup_interval: Duration::from_secs(60),
        }
    }
}

/// How long to sleep given the time remaining until the next timer fires.
///
/// Kept in milliseconds. Truncating to whole seconds meant a timer due in 900ms
/// resolved to a zero-length wait, which then clamped up to `min_sleep_duration`
/// — so the loop woke and re-queried the queue roughly every 10ms until the
/// timer came due. Harmless for correctness, since it errs early, but it is a
/// database round trip per iteration, and sub-second timers are supported.
fn sleep_for_next_fire(millis_until_fire: i128, min: Duration, max: Duration) -> Duration {
    let until_fire = Duration::from_millis(millis_until_fire.max(0) as u64);
    until_fire.max(min).min(max)
}

/// Metrics for the timer processor
#[derive(Debug, Clone, Default)]
pub struct TimerMetrics {
    /// Total timers fired successfully
    pub timers_fired: u64,

    /// Total timers failed
    pub timers_failed: u64,

    /// Total timers moved to DLQ
    pub timers_dlq: u64,

    /// Total stuck timers recovered
    pub stuck_recovered: u64,

    /// Current pending timers
    pub pending_timers: usize,
}

/// Timer processor for background timer execution
pub struct TimerProcessor {
    /// Timer queue
    timer_queue: TimerQueue,

    /// Coordination repository (for DLQ operations)
    coordination_repo: Arc<CoordinationRepository>,

    /// Configuration
    config: TimerProcessorConfig,

    /// Processor metrics
    metrics: Arc<Mutex<TimerMetrics>>,

    /// Shutdown signal
    shutdown: Arc<Mutex<bool>>,

    /// Signalled when a timer is scheduled, to cut the idle sleep short.
    wake: Arc<Notify>,
}

impl TimerProcessor {
    /// Create a new timer processor
    pub fn new(
        timer_queue: TimerQueue,
        coordination_repo: Arc<CoordinationRepository>,
        config: TimerProcessorConfig,
    ) -> Self {
        Self {
            timer_queue,
            coordination_repo,
            config,
            metrics: Arc::new(Mutex::new(TimerMetrics::default())),
            shutdown: Arc::new(Mutex::new(false)),
            wake: Arc::new(Notify::new()),
        }
    }

    /// Handle used to tell the processor a timer has just been scheduled.
    ///
    /// Whoever writes a timer signals this; the processor then re-reads the
    /// queue instead of sleeping out its current interval.
    pub fn wake_handle(&self) -> Arc<Notify> {
        self.wake.clone()
    }

    /// Create with default configuration
    pub fn with_defaults(
        timer_queue: TimerQueue,
        coordination_repo: Arc<CoordinationRepository>,
    ) -> Self {
        Self::new(
            timer_queue,
            coordination_repo,
            TimerProcessorConfig::default(),
        )
    }

    /// Start the processor (spawns background tasks)
    pub async fn start<F>(self: Arc<Self>, executor: F) -> OrcherResult<()>
    where
        F: Fn(
                TimerTask,
            )
                -> std::pin::Pin<Box<dyn std::future::Future<Output = OrcherResult<()>> + Send>>
            + Send
            + Sync
            + 'static,
    {
        let executor = Arc::new(executor);

        info!("Starting timer processor");

        // Spawn processing loop
        let processor = self.clone();
        let exec = executor.clone();
        tokio::spawn(async move {
            processor.run_processing_loop(exec).await;
        });

        // Spawn cleanup loop
        let processor = self.clone();
        tokio::spawn(async move {
            processor.run_cleanup_loop().await;
        });

        Ok(())
    }

    /// Signal shutdown
    pub async fn shutdown(&self) {
        info!("Shutting down timer processor");
        *self.shutdown.lock().await = true;
    }

    /// Get current metrics
    pub async fn get_metrics(&self) -> TimerMetrics {
        self.metrics.lock().await.clone()
    }

    /// Main processing loop
    async fn run_processing_loop<F>(self: Arc<Self>, executor: Arc<F>)
    where
        F: Fn(
                TimerTask,
            )
                -> std::pin::Pin<Box<dyn std::future::Future<Output = OrcherResult<()>> + Send>>
            + Send
            + Sync
            + 'static,
    {
        loop {
            if *self.shutdown.lock().await {
                info!("Processing loop received shutdown signal");
                break;
            }

            // Claim expired timers
            // TODO: Pass organization_id from context when multi-tenant mode is enabled
            let timers = match self
                .timer_queue
                .claim_expired_tasks(None, self.config.batch_size)
                .await
            {
                Ok(timers) => timers,
                Err(e) => {
                    error!("Failed to claim expired timers: {}", e);
                    sleep(Duration::from_secs(1)).await;
                    continue;
                }
            };

            if !timers.is_empty() {
                debug!(count = timers.len(), "Processing expired timers");

                // Process all expired timers
                for timer in timers {
                    if *self.shutdown.lock().await {
                        break;
                    }

                    if let Err(e) = self.execute_timer(&timer, executor.clone()).await {
                        error!(
                            timer_id = %timer.task_id,
                            workflow_id = %timer.workflow_id,
                            error = %e,
                            "Timer execution failed"
                        );
                    }
                }

                // After processing, immediately check for more expired timers
                continue;
            }

            // No expired timers, calculate sleep duration
            let sleep_duration = match self.calculate_sleep_duration().await {
                Ok(duration) => duration,
                Err(e) => {
                    error!("Failed to calculate sleep duration: {}", e);
                    self.config.max_sleep_duration
                }
            };

            debug!(
                sleep_secs = sleep_duration.as_secs(),
                "Sleeping until next timer"
            );

            // Sleep until the next timer is due, a timer is scheduled, or we
            // are told to stop.
            //
            // Waiting out the full interval is what made `sleep(1s)` take 30
            // seconds: `calculate_sleep_duration` returns the maximum when the
            // queue is empty, and a timer created a moment later was not
            // noticed until that sleep expired. Nothing signalled the loop, so
            // the delay was the sleep bound rather than the timer's duration.
            tokio::select! {
                _ = self.wake.notified() => {
                    debug!("Woken by a newly scheduled timer");
                }
                _ = sleep(sleep_duration) => {}
                _ = async {
                    loop {
                        if *self.shutdown.lock().await {
                            break;
                        }
                        sleep(Duration::from_millis(100)).await;
                    }
                } => {}
            }
        }
    }

    /// Calculate optimal sleep duration based on next timer
    async fn calculate_sleep_duration(&self) -> OrcherResult<Duration> {
        // TODO: Pass organization_id from context when multi-tenant mode is enabled
        let next_fire = self.timer_queue.get_next_fire_time(None).await?;

        let sleep_duration = match next_fire {
            Some(fire_time) => {
                let now = Timestamptz::now();
                let millis_until_fire = if fire_time > now {
                    (fire_time - now).as_millis()
                } else {
                    // Timer already expired, check immediately
                    0
                };

                sleep_for_next_fire(
                    millis_until_fire,
                    self.config.min_sleep_duration,
                    self.config.max_sleep_duration,
                )
            }
            None => {
                // No pending timers, use max sleep
                self.config.max_sleep_duration
            }
        };

        Ok(sleep_duration)
    }

    /// Execute a single timer
    async fn execute_timer<F>(&self, timer: &TimerTask, executor: Arc<F>) -> OrcherResult<()>
    where
        F: Fn(
                TimerTask,
            )
                -> std::pin::Pin<Box<dyn std::future::Future<Output = OrcherResult<()>> + Send>>
            + Send
            + Sync
            + 'static,
    {
        debug!(
            timer_id = %timer.task_id,
            timer_type = %timer.task_type,
            attempt = timer.attempt_count,
            fire_at = %timer.fire_at,
            "Executing timer"
        );

        // Execute the timer using the provided executor
        let result = executor(timer.clone()).await;

        match result {
            Ok(()) => {
                // Mark as completed
                if let Err(e) = self.timer_queue.mark_completed(timer.task_id).await {
                    error!(
                        timer_id = %timer.task_id,
                        error = %e,
                        "Failed to mark timer as completed"
                    );
                    return Err(e);
                }

                // Update metrics
                let mut metrics = self.metrics.lock().await;
                metrics.timers_fired += 1;

                debug!(timer_id = %timer.task_id, "Timer executed successfully");
                Ok(())
            }
            Err(e) => {
                // Mark as failed (handles retry logic)
                let error_msg = e.to_string();

                match self
                    .timer_queue
                    .mark_failed(timer.task_id, error_msg.clone())
                    .await
                {
                    Ok(can_retry) => {
                        if !can_retry {
                            // Move to DLQ after exhausting retries using coordination repository
                            let dlq_error_details = serde_json::json!({
                                "error": error_msg,
                                "attempt_count": timer.attempt_count,
                                "max_retries": timer.max_retries,
                                "fire_at": timer.fire_at,
                            });

                            match self
                                .coordination_repo
                                .move_timer_task_to_dlq(timer, dlq_error_details)
                                .await
                            {
                                Ok(dlq_task) => {
                                    info!(
                                        timer_id = %timer.task_id,
                                        dlq_id = %dlq_task.dlq_id,
                                        "Moved timer to DLQ after exhausting retries"
                                    );
                                    let mut metrics = self.metrics.lock().await;
                                    metrics.timers_dlq += 1;
                                }
                                Err(dlq_err) => {
                                    error!(
                                        timer_id = %timer.task_id,
                                        error = %dlq_err,
                                        "Failed to move timer to DLQ"
                                    );
                                }
                            }
                        }

                        let mut metrics = self.metrics.lock().await;
                        metrics.timers_failed += 1;

                        Err(e)
                    }
                    Err(mark_err) => {
                        error!(
                            timer_id = %timer.task_id,
                            error = %mark_err,
                            "Failed to mark timer as failed"
                        );
                        Err(mark_err)
                    }
                }
            }
        }
    }

    /// Cleanup loop for stuck timers
    async fn run_cleanup_loop(self: Arc<Self>) {
        let mut last_cleanup = std::time::Instant::now();

        loop {
            sleep(Duration::from_secs(10)).await; // Check every 10 seconds

            if *self.shutdown.lock().await {
                info!("Cleanup loop received shutdown signal");
                break;
            }

            if last_cleanup.elapsed() < self.config.cleanup_interval {
                continue;
            }

            last_cleanup = std::time::Instant::now();

            match self
                .timer_queue
                .cleanup_stuck_tasks(self.config.stuck_threshold_secs)
                .await
            {
                Ok(recovered_count) => {
                    if recovered_count > 0 {
                        let mut metrics = self.metrics.lock().await;
                        metrics.stuck_recovered += recovered_count as u64;

                        warn!(recovered = recovered_count, "Recovered stuck timers");
                    }
                }
                Err(e) => {
                    error!("Failed to cleanup stuck timers: {}", e);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {

    #[test]
    fn sub_second_wait_is_kept_not_truncated() {
        // The bug: 900ms truncated to 0s, clamped up to the 10ms floor, and the
        // loop re-queried the database ~90 times before the timer came due.
        let d = sleep_for_next_fire(900, Duration::from_millis(10), Duration::from_secs(1));
        assert_eq!(d, Duration::from_millis(900));
    }

    #[test]
    fn fractional_second_keeps_its_remainder() {
        let d = sleep_for_next_fire(1_500, Duration::from_millis(10), Duration::from_secs(30));
        assert_eq!(d, Duration::from_millis(1_500));
    }

    #[test]
    fn never_sleeps_below_the_floor() {
        let d = sleep_for_next_fire(1, Duration::from_millis(10), Duration::from_secs(1));
        assert_eq!(d, Duration::from_millis(10));
    }

    #[test]
    fn never_sleeps_past_the_backstop() {
        let d = sleep_for_next_fire(60_000, Duration::from_millis(10), Duration::from_secs(1));
        assert_eq!(d, Duration::from_secs(1));
    }

    #[test]
    fn an_overdue_timer_waits_only_the_floor() {
        let d = sleep_for_next_fire(-5_000, Duration::from_millis(10), Duration::from_secs(1));
        assert_eq!(d, Duration::from_millis(10));
    }

    use super::*;
    use sqlx::PgPool;
    use std::sync::atomic::{AtomicUsize, Ordering};

    async fn create_test_pool() -> PgPool {
        todo!("Test pool creation")
    }

    #[tokio::test]
    #[ignore] // Requires database
    async fn test_processor_creation() {
        let pool = Arc::new(create_test_pool().await);
        let timer_repo = Arc::new(
            orcher_storage::repositories::domains::coordination::TimerRepository::new(pool.clone()),
        );
        let timer_queue = TimerQueue::new(timer_repo);
        let coordination_repo = Arc::new(CoordinationRepository::new(pool));
        let processor = TimerProcessor::with_defaults(timer_queue, coordination_repo);

        let metrics = processor.get_metrics().await;
        assert_eq!(metrics.timers_fired, 0);
        assert_eq!(metrics.timers_failed, 0);
    }

    #[tokio::test]
    #[ignore] // Requires database
    async fn test_sleep_duration_calculation() {
        let pool = Arc::new(create_test_pool().await);
        let timer_repo = Arc::new(
            orcher_storage::repositories::domains::coordination::TimerRepository::new(pool.clone()),
        );
        let timer_queue = TimerQueue::new(timer_repo);
        let coordination_repo = Arc::new(CoordinationRepository::new(pool));
        let processor = TimerProcessor::with_defaults(timer_queue, coordination_repo);

        // No timers should return max sleep
        let duration = processor.calculate_sleep_duration().await.unwrap();
        assert_eq!(duration, processor.config.max_sleep_duration);
    }

    #[tokio::test]
    #[ignore] // Requires database
    async fn test_timer_execution() {
        let pool = Arc::new(create_test_pool().await);
        let timer_repo = Arc::new(
            orcher_storage::repositories::domains::coordination::TimerRepository::new(pool.clone()),
        );
        let timer_queue = TimerQueue::new(timer_repo);
        let coordination_repo = Arc::new(CoordinationRepository::new(pool));
        let processor = Arc::new(TimerProcessor::with_defaults(
            timer_queue,
            coordination_repo,
        ));

        let execution_count = Arc::new(AtomicUsize::new(0));
        let exec_count = execution_count.clone();

        // Simple executor that counts executions
        let executor = move |_timer: TimerTask| {
            let count = exec_count.clone();
            Box::pin(async move {
                count.fetch_add(1, Ordering::SeqCst);
                Ok(())
            })
                as std::pin::Pin<Box<dyn std::future::Future<Output = OrcherResult<()>> + Send>>
        };

        let processor_clone = Arc::clone(&processor);
        processor.start(executor).await.unwrap();

        // Give it time to process
        sleep(Duration::from_millis(100)).await;

        processor_clone.shutdown().await;
    }
}
