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
    /// How soon a tick held by the `queue` policy is looked at again, to fire
    /// once the run it waits for has ended.
    pub queue_recheck: Duration,
}

impl Default for ScheduleProcessorConfig {
    fn default() -> Self {
        Self {
            batch_size: 50,
            max_sleep_duration: Duration::from_secs(30),
            min_sleep_duration: Duration::from_millis(100),
            stuck_threshold_secs: 300,
            cleanup_interval: Duration::from_secs(60),
            queue_recheck: Duration::from_secs(5),
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
    /// Ticks the overlap policy skipped.
    pub schedules_skipped: u64,
    /// Ticks the `queue` policy held for a run still open.
    pub schedules_held: u64,
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
    /// The executor receives a `Schedule` and the runs its fire replaces (the
    /// open runs of a `replace` schedule, by execution id, to be cancelled),
    /// starts the schedule's workflow, and returns the started run's row id
    /// (`workflow_executions.id`).
    ///
    /// This method spawns two background loops:
    /// - Processing loop: claims and fires due schedules
    /// - Cleanup loop: recovers stuck schedules
    pub async fn start<F>(self: Arc<Self>, executor: F) -> OrcherResult<()>
    where
        F: Fn(
                Schedule,
                Vec<String>,
            ) -> Pin<Box<dyn Future<Output = OrcherResult<uuid::Uuid>> + Send>>
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
        F: Fn(
                Schedule,
                Vec<String>,
            ) -> Pin<Box<dyn Future<Output = OrcherResult<uuid::Uuid>> + Send>>
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

    /// Process one schedule this processor has claimed: decide its overlap
    /// policy against the runs it started that are still open, then fire it,
    /// skip the tick, or hold it.
    pub async fn process_schedule<F>(
        &self,
        schedule: &Schedule,
        executor: Arc<F>,
    ) -> OrcherResult<()>
    where
        F: Fn(
                Schedule,
                Vec<String>,
            ) -> Pin<Box<dyn Future<Output = OrcherResult<uuid::Uuid>> + Send>>
            + Send
            + Sync
            + 'static,
    {
        let replacing = match self.decide(schedule).await? {
            Overlap::Fire { replacing } => replacing,
            Overlap::Skip => {
                debug!(
                    schedule_id = %schedule.id,
                    policy = ?schedule.overlap_policy,
                    "Skipping a tick: the schedule's previous run is still open"
                );
                let next = self.compute_next_fire_time(schedule)?;
                self.schedule_queue
                    .release_unfired(schedule.id, next)
                    .await?;
                self.metrics.lock().await.schedules_skipped += 1;
                return Ok(());
            }
            Overlap::Hold => {
                debug!(
                    schedule_id = %schedule.id,
                    "Holding a tick until the schedule's previous run ends"
                );
                let recheck = jiff::SignedDuration::try_from(self.config.queue_recheck)
                    .unwrap_or(jiff::SignedDuration::from_secs(5));
                self.schedule_queue
                    .release_unfired(schedule.id, Some(Timestamptz::now() + recheck))
                    .await?;
                self.metrics.lock().await.schedules_held += 1;
                return Ok(());
            }
        };

        // Fire the schedule (call executor to start workflow)
        let fire_result = executor(schedule.clone(), replacing).await;

        // Record the fire, and the run it started if it started one.
        let scheduled_at = schedule.next_run_at.unwrap_or_else(Timestamptz::now);
        let run = fire_result.as_ref().ok().copied();
        // The record is what the overlap policy reads the run back from, so a
        // fire that could not be recorded is said so rather than dropped.
        if let Err(e) = self
            .schedule_queue
            .insert_execution(schedule.id, schedule.run_count + 1, scheduled_at, run)
            .await
        {
            warn!(
                schedule_id = %schedule.id,
                error = %e,
                "A schedule fired, but the fire could not be recorded; its overlap policy will not see the run"
            );
        }

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

    /// What the schedule's overlap policy makes of a tick, given the runs the
    /// schedule started that are still open. Read for every policy, so each
    /// fire is settled once its runs end, whatever the policy.
    async fn decide(&self, schedule: &Schedule) -> OrcherResult<Overlap> {
        let open = self.schedule_queue.open_runs(schedule.id).await?;
        Ok(overlap(
            schedule.overlap_policy,
            schedule.max_instances,
            open,
        ))
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

/// What a schedule's overlap policy makes of a tick.
#[derive(Debug, PartialEq, Eq)]
enum Overlap {
    /// Start a run, cancelling these first (execution ids).
    Fire { replacing: Vec<String> },
    /// Drop the tick; the schedule is next due at its next cron time.
    Skip,
    /// Keep the tick until the open run ends. Ticks that come due meanwhile
    /// are not stacked behind it: one run waits, then the cron carries on.
    Hold,
}

/// The overlap policies, as the schedules API documents them, given the
/// schedule's runs still `open`.
fn overlap(policy: OverlapPolicy, max_instances: i32, open: Vec<String>) -> Overlap {
    match policy {
        OverlapPolicy::Skip if !open.is_empty() => Overlap::Skip,
        OverlapPolicy::Queue if !open.is_empty() => Overlap::Hold,
        // Up to `max_instances` at once, which the schema keeps at one or more.
        OverlapPolicy::Parallel if open.len() >= max_instances.max(1) as usize => Overlap::Skip,
        OverlapPolicy::Replace => Overlap::Fire { replacing: open },
        _ => Overlap::Fire {
            replacing: Vec::new(),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn runs(n: usize) -> Vec<String> {
        (0..n).map(|i| format!("run-{i}")).collect()
    }

    #[test]
    fn every_policy_fires_when_nothing_is_open() {
        for policy in [
            OverlapPolicy::Skip,
            OverlapPolicy::Queue,
            OverlapPolicy::Replace,
            OverlapPolicy::Parallel,
        ] {
            assert_eq!(
                overlap(policy, 1, runs(0)),
                Overlap::Fire { replacing: vec![] },
                "{policy:?}"
            );
        }
    }

    #[test]
    fn an_open_run_is_skipped_held_replaced_or_joined() {
        assert_eq!(overlap(OverlapPolicy::Skip, 1, runs(1)), Overlap::Skip);
        assert_eq!(overlap(OverlapPolicy::Queue, 1, runs(1)), Overlap::Hold);
        assert_eq!(
            overlap(OverlapPolicy::Replace, 1, runs(2)),
            Overlap::Fire { replacing: runs(2) }
        );
        assert_eq!(
            overlap(OverlapPolicy::Parallel, 2, runs(1)),
            Overlap::Fire { replacing: vec![] }
        );
    }

    #[test]
    fn parallel_runs_up_to_max_instances() {
        assert_eq!(overlap(OverlapPolicy::Parallel, 2, runs(2)), Overlap::Skip);
        assert_eq!(overlap(OverlapPolicy::Parallel, 1, runs(1)), Overlap::Skip);
        assert_eq!(
            overlap(OverlapPolicy::Parallel, 50, runs(49)),
            Overlap::Fire { replacing: vec![] }
        );
    }

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
        assert_eq!(metrics.schedules_held, 0);
        assert_eq!(metrics.schedules_failed, 0);
        assert_eq!(metrics.stuck_recovered, 0);
    }
}
