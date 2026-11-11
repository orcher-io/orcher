//! Background schedule processor for cron-based workflow scheduling.
//!
//! Claims due schedules using `FOR UPDATE SKIP LOCKED`, checks overlap policies,
//! fires workflow executions, and computes the next fire time. Modeled on TimerProcessor.

use crate::engine::cron;
use crate::engine::queues::ScheduleQueue;
use orcher_common::error::OrcherResult;
use orcher_common::types::time::Timestamptz;
use orcher_storage::schema::types::{OverlapPolicy, Schedule, ScheduleStatus};

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::time::{sleep, timeout};
use tracing::{debug, error, info, warn};

// ============================================================================
// Configuration
// ============================================================================

/// Configuration for the ScheduleProcessor.
#[derive(Debug, Clone)]
pub struct ScheduleProcessorConfig {
    /// Maximum number of schedules to claim per iteration.
    pub batch_size: usize,
    /// Maximum sleep duration between checks.
    pub max_sleep_duration: Duration,
    /// Minimum sleep duration (prevents tight loops).
    pub min_sleep_duration: Duration,
    /// Schedules in `processing` longer than this (seconds) are considered stuck.
    pub stuck_threshold_secs: i64,
    /// How often to run the cleanup loop.
    pub cleanup_interval: Duration,
}

impl Default for ScheduleProcessorConfig {
    fn default() -> Self {
        Self {
            batch_size: 50,
            max_sleep_duration: Duration::from_secs(30),
            min_sleep_duration: Duration::from_millis(100),
            stuck_threshold_secs: 300,
            cleanup_interval: Duration::from_secs(60),
        }
    }
}

// ============================================================================
// Metrics
// ============================================================================

/// Metrics tracked by the ScheduleProcessor.
#[derive(Debug, Clone, Default)]
pub struct ScheduleMetrics {
    /// Schedules successfully fired (workflow started).
    pub schedules_fired: u64,
    /// Schedules skipped due to overlap policy.
    pub schedules_skipped: u64,
    /// Schedules that failed to fire.
    pub schedules_failed: u64,
    /// Stuck schedules recovered by cleanup loop.
    pub stuck_recovered: u64,
}

// ============================================================================
// Processor
// ============================================================================

/// Background processor that fires scheduled workflows based on cron expressions.
///
/// Uses PostgreSQL `FOR UPDATE SKIP LOCKED` for distributed claiming.
/// Multiple orchestrator instances can safely run this concurrently.
pub struct ScheduleProcessor {
    schedule_queue: ScheduleQueue,
    config: ScheduleProcessorConfig,
    metrics: Arc<Mutex<ScheduleMetrics>>,
    shutdown: Arc<Mutex<bool>>,
}

impl ScheduleProcessor {
    /// Create a new ScheduleProcessor.
    pub fn new(schedule_queue: ScheduleQueue, config: ScheduleProcessorConfig) -> Self {
        Self {
            schedule_queue,
            config,
            metrics: Arc::new(Mutex::new(ScheduleMetrics::default())),
            shutdown: Arc::new(Mutex::new(false)),
        }
    }

    /// Create with default configuration.
    pub fn with_defaults(schedule_queue: ScheduleQueue) -> Self {
        Self::new(schedule_queue, ScheduleProcessorConfig::default())
    }

    /// Get current metrics.
    pub async fn get_metrics(&self) -> ScheduleMetrics {
        self.metrics.lock().await.clone()
    }

    /// Signal graceful shutdown.
    pub async fn shutdown(&self) {
        info!("Shutting down schedule processor");
        *self.shutdown.lock().await = true;
    }

    /// Start the processor with a custom executor function.
    ///
    /// The executor receives a `Schedule` and should start the corresponding
    /// workflow execution. It returns `Ok(())` on success or an error.
    ///
    /// This method spawns two background loops:
    /// - Processing loop: claims and fires due schedules
    /// - Cleanup loop: recovers stuck schedules
    pub async fn start<F>(self: Arc<Self>, executor: F) -> OrcherResult<()>
    where
        F: Fn(Schedule) -> Pin<Box<dyn Future<Output = OrcherResult<uuid::Uuid>> + Send>>
            + Send
            + Sync
            + 'static,
    {
        info!(
            batch_size = self.config.batch_size,
            max_sleep_ms = self.config.max_sleep_duration.as_millis() as u64,
            "Starting schedule processor"
        );

        let executor = Arc::new(executor);

        // Spawn cleanup loop
        let cleanup_self = self.clone();
        tokio::spawn(async move {
            cleanup_self.run_cleanup_loop().await;
        });

        // Run main processing loop
        self.run_processing_loop(executor).await;

        info!("Schedule processor stopped");
        Ok(())
    }

    /// Main processing loop: claim → check overlap → execute → update → sleep.
    async fn run_processing_loop<F>(self: Arc<Self>, executor: Arc<F>)
    where
        F: Fn(Schedule) -> Pin<Box<dyn Future<Output = OrcherResult<uuid::Uuid>> + Send>>
            + Send
            + Sync
            + 'static,
    {
        loop {
            // Check shutdown
            if *self.shutdown.lock().await {
                break;
            }

            // Claim due schedules
            let schedules = match self
                .schedule_queue
                .claim_ready_schedules(self.config.batch_size)
                .await
            {
                Ok(s) => s,
                Err(e) => {
                    error!("Failed to claim schedules: {}", e);
                    sleep(Duration::from_secs(1)).await;
                    continue;
                }
            };

            // Process claimed schedules
            if !schedules.is_empty() {
                debug!(count = schedules.len(), "Processing due schedules");

                for schedule in schedules {
                    if *self.shutdown.lock().await {
                        break;
                    }

                    if let Err(e) = self.process_schedule(&schedule, executor.clone()).await {
                        error!(
                            schedule_id = %schedule.id,
                            schedule_name = %schedule.name,
                            error = %e,
                            "Schedule execution failed"
                        );
                    }
                }

                // Hot-loop: immediately check for more without sleeping
                continue;
            }

            // Nothing to process — sleep until next fire time
            let sleep_duration = match self.calculate_sleep_duration().await {
                Ok(d) => d,
                Err(e) => {
                    error!("Failed to calculate sleep duration: {}", e);
                    self.config.max_sleep_duration
                }
            };

            debug!(
                sleep_ms = sleep_duration.as_millis() as u64,
                "Sleeping until next schedule"
            );

            // Interruptible sleep
            let _ = timeout(sleep_duration, async {
                loop {
                    if *self.shutdown.lock().await {
                        break;
                    }
                    sleep(Duration::from_millis(100)).await;
                }
            })
            .await;
        }
    }

    /// Process a single claimed schedule.
    async fn process_schedule<F>(&self, schedule: &Schedule, executor: Arc<F>) -> OrcherResult<()>
    where
        F: Fn(Schedule) -> Pin<Box<dyn Future<Output = OrcherResult<uuid::Uuid>> + Send>>
            + Send
            + Sync
            + 'static,
    {
        // Check overlap policy
        if !self.should_fire(schedule).await? {
            debug!(
                schedule_id = %schedule.id,
                policy = ?schedule.overlap_policy,
                "Skipping schedule due to overlap policy"
            );

            // Compute next fire time and reset to active
            let next = self.compute_next_fire_time(schedule)?;
            self.schedule_queue.mark_fired(schedule.id, next).await?;

            let mut metrics = self.metrics.lock().await;
            metrics.schedules_skipped += 1;

            return Ok(());
        }

        // Fire the schedule (call executor to start workflow)
        let fire_result = executor(schedule.clone()).await;

        // Record execution with the workflow execution ID (if successful)
        let scheduled_at = schedule.next_run_at.unwrap_or_else(Timestamptz::now);
        let workflow_execution_id = fire_result.as_ref().ok().copied();
        let _ = self
            .schedule_queue
            .insert_execution(
                schedule.id,
                schedule.run_count + 1,
                scheduled_at,
                workflow_execution_id,
            )
            .await;

        // Compute next fire time
        let next = self.compute_next_fire_time(schedule)?;

        match fire_result {
            Ok(_workflow_execution_id) => {
                // Update schedule: last_run_at, next_run_at, run_count, status → active
                self.schedule_queue.mark_fired(schedule.id, next).await?;

                let mut metrics = self.metrics.lock().await;
                metrics.schedules_fired += 1;

                debug!(
                    schedule_id = %schedule.id,
                    schedule_name = %schedule.name,
                    "Schedule fired successfully"
                );

                // Check if schedule should be deactivated
                self.check_completion(schedule).await?;

                Ok(())
            }
            Err(e) => {
                // Mark failed but reset to active for next attempt
                self.schedule_queue.mark_failed(schedule.id).await?;
                // Still compute next_run_at so it doesn't fire the same stale time
                if let Some(next_time) = next {
                    let _ = self
                        .schedule_queue
                        .update_status(schedule.id, ScheduleStatus::Active)
                        .await;
                    // mark_failed already resets to active, but we need to set next_run_at
                    // mark_fired does both in one query, reuse it
                    let _ = self
                        .schedule_queue
                        .mark_fired(schedule.id, Some(next_time))
                        .await;
                }

                let mut metrics = self.metrics.lock().await;
                metrics.schedules_failed += 1;

                Err(e)
            }
        }
    }

    /// Check overlap policy — should this schedule fire given active instances?
    async fn should_fire(&self, schedule: &Schedule) -> OrcherResult<bool> {
        match schedule.overlap_policy {
            OverlapPolicy::Parallel => Ok(true),
            OverlapPolicy::Skip => {
                let active = self
                    .schedule_queue
                    .count_active_instances(schedule.id)
                    .await?;
                Ok(active == 0)
            }
            OverlapPolicy::Replace => {
                // For v1, treat Replace as "allow" — proper cancellation of
                // running workflows requires integration with WorkflowService.
                Ok(true)
            }
            OverlapPolicy::Queue => {
                // For v1, treat Queue as Skip — buffering requires additional
                // state management.
                let active = self
                    .schedule_queue
                    .count_active_instances(schedule.id)
                    .await?;
                Ok(active == 0)
            }
        }
    }

    /// Compute the next fire time for a schedule.
    fn compute_next_fire_time(&self, schedule: &Schedule) -> OrcherResult<Option<Timestamptz>> {
        let now = Timestamptz::now();

        // Check if schedule has reached its limits
        if schedule.has_reached_max_runs() {
            return Ok(None);
        }
        if let Some(end_date) = schedule.end_date {
            if now >= end_date {
                return Ok(None);
            }
        }

        cron::next_fire_time(&schedule.cron_expression, &schedule.timezone, now)
    }

    /// Check if a schedule should be marked as completed (max_runs or end_date reached).
    async fn check_completion(&self, schedule: &Schedule) -> OrcherResult<()> {
        // run_count was incremented by mark_fired, so check max_runs against run_count + 1
        let should_complete = if let Some(max_runs) = schedule.max_runs {
            (schedule.run_count + 1) >= max_runs
        } else {
            false
        };

        let past_end_date = if let Some(end_date) = schedule.end_date {
            Timestamptz::now() >= end_date
        } else {
            false
        };

        if should_complete || past_end_date {
            info!(
                schedule_id = %schedule.id,
                schedule_name = %schedule.name,
                "Schedule completed (max_runs or end_date reached)"
            );
            self.schedule_queue
                .update_status(schedule.id, ScheduleStatus::Completed)
                .await?;
        }

        Ok(())
    }

    /// Calculate how long to sleep before the next check.
    async fn calculate_sleep_duration(&self) -> OrcherResult<Duration> {
        let next_fire = self.schedule_queue.get_next_fire_time().await?;

        let duration = match next_fire {
            Some(next) => {
                let now = Timestamptz::now();
                if next <= now {
                    // Already due — process immediately
                    self.config.min_sleep_duration
                } else {
                    let diff_secs = next.timestamp() - now.timestamp();
                    let diff = Duration::from_secs(diff_secs.max(0) as u64);
                    diff.min(self.config.max_sleep_duration)
                        .max(self.config.min_sleep_duration)
                }
            }
            None => self.config.max_sleep_duration,
        };

        Ok(duration)
    }

    /// Cleanup loop: recovers stuck schedules and completes exhausted ones.
    async fn run_cleanup_loop(self: Arc<Self>) {
        let mut last_cleanup = std::time::Instant::now();

        loop {
            sleep(Duration::from_secs(10)).await;

            if *self.shutdown.lock().await {
                break;
            }

            if last_cleanup.elapsed() < self.config.cleanup_interval {
                continue;
            }

            last_cleanup = std::time::Instant::now();

            // Recover stuck schedules
            match self
                .schedule_queue
                .cleanup_stuck_schedules(self.config.stuck_threshold_secs)
                .await
            {
                Ok(count) => {
                    if count > 0 {
                        let mut metrics = self.metrics.lock().await;
                        metrics.stuck_recovered += count as u64;
                        warn!(recovered = count, "Recovered stuck schedules");
                    }
                }
                Err(e) => {
                    error!("Failed to cleanup stuck schedules: {}", e);
                }
            }

            // Complete exhausted schedules (max_runs or past end_date)
            match self.schedule_queue.find_schedules_to_complete().await {
                Ok(schedules) => {
                    for schedule in schedules {
                        if let Err(e) = self
                            .schedule_queue
                            .update_status(schedule.id, ScheduleStatus::Completed)
                            .await
                        {
                            error!(
                                schedule_id = %schedule.id,
                                error = %e,
                                "Failed to complete exhausted schedule"
                            );
                        } else {
                            info!(
                                schedule_id = %schedule.id,
                                schedule_name = %schedule.name,
                                "Completed exhausted schedule"
                            );
                        }
                    }
                }
                Err(e) => {
                    error!("Failed to find schedules to complete: {}", e);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = ScheduleProcessorConfig::default();
        assert_eq!(config.batch_size, 50);
        assert_eq!(config.max_sleep_duration, Duration::from_secs(30));
        assert_eq!(config.stuck_threshold_secs, 300);
    }

    #[test]
    fn test_default_metrics() {
        let metrics = ScheduleMetrics::default();
        assert_eq!(metrics.schedules_fired, 0);
        assert_eq!(metrics.schedules_skipped, 0);
        assert_eq!(metrics.schedules_failed, 0);
        assert_eq!(metrics.stuck_recovered, 0);
    }
}
