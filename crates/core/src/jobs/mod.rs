//! Periodic background jobs.
//!
//! Metric names here are declared as constants and checked by a test rather
//! than written inline at the call site. They were written inline, in dotted
//! form — `jobs.data_retention.runs_total` — which Prometheus does not accept:
//! a name must match `[a-zA-Z_:][a-zA-Z0-9_:]*`. The cost depended entirely on
//! which recorder happened to be installed. With the Prometheus recorder
//! (the `prometheus` feature) the first emission panicked the job's task, so
//! the job ran once and never again; without it the calls were silent no-ops
//! and the metrics simply did not exist. Neither failure said anything, and
//! retention quietly not running is unbounded table growth.

use sqlx::PgPool;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tracing::{error, info};

pub mod data_retention;
pub mod metrics_aggregation;

/// Start all background jobs
///
/// Spawns a tokio task for each enabled job. Each job runs on its own
/// interval and can be shut down gracefully via the shutdown signal.
///
/// # Arguments
///
/// * `pool` - Database connection pool
/// * `config` - Configuration for all jobs
/// * `shutdown` - Watch receiver for shutdown signal
///
/// # Returns
///
/// Vector of join handles for all spawned jobs
pub async fn start_background_jobs(
    pool: Arc<PgPool>,
    config: BackgroundJobsConfig,
    shutdown: watch::Receiver<bool>,
) -> Vec<JoinHandle<()>> {
    info!("Starting background jobs system");

    let mut handles = Vec::new();

    // Job 1: Metrics Aggregation
    // Aggregates workflow execution metrics for analytics and monitoring
    if config.metrics_enabled {
        info!(
            "Enabling metrics aggregation job (interval: {}s)",
            config.metrics_interval_secs
        );

        let pool = pool.clone();
        let interval = Duration::from_secs(config.metrics_interval_secs);
        let mut shutdown_rx = shutdown.clone();

        handles.push(tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);

            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        if let Err(e) = metrics_aggregation::run(&pool).await {
                            error!("Metrics aggregation failed: {}", e);
                            // Continue running despite errors
                        }
                    }
                    _ = shutdown_rx.changed() => {
                        info!("Metrics aggregation job stopping gracefully");
                        break;
                    }
                }
            }
        }));
    } else {
        info!("Metrics aggregation job disabled");
    }

    // Job 2: Data Retention
    // Cleans up old workflow executions, logs, and tasks
    if config.retention_enabled {
        info!(
            "Enabling data retention job (interval: {}s)",
            config.retention_interval_secs
        );

        let pool = pool.clone();
        let interval = Duration::from_secs(config.retention_interval_secs);
        let retention_config = config.retention_days.clone();
        let mut shutdown_rx = shutdown.clone();

        handles.push(tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);

            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        if let Err(e) = data_retention::run(&pool, &retention_config).await {
                            error!("Data retention failed: {}", e);
                            // Continue running despite errors
                        }
                    }
                    _ = shutdown_rx.changed() => {
                        info!("Data retention job stopping gracefully");
                        break;
                    }
                }
            }
        }));
    } else {
        info!("Data retention job disabled");
    }

    info!(
        "Background jobs system started with {} active jobs",
        handles.len()
    );

    handles
}

/// Configuration for all background jobs
#[derive(Debug, Clone)]
pub struct BackgroundJobsConfig {
    // Metrics aggregation job
    /// Enable metrics aggregation job
    pub metrics_enabled: bool,
    /// Interval between aggregation runs (in seconds)
    pub metrics_interval_secs: u64,

    // Data retention job
    /// Enable data retention job
    pub retention_enabled: bool,
    /// Interval between retention runs (in seconds)
    pub retention_interval_secs: u64,
    /// Retention periods for different data types
    pub retention_days: RetentionDays,
}

impl Default for BackgroundJobsConfig {
    fn default() -> Self {
        Self {
            // Metrics aggregation: run every hour
            metrics_enabled: true,
            metrics_interval_secs: 3600, // 1 hour

            // Data retention: run daily
            retention_enabled: true,
            retention_interval_secs: 86400, // 1 day (24 hours)

            retention_days: RetentionDays::default(),
        }
    }
}

/// Retention periods for different data types
#[derive(Debug, Clone)]
pub struct RetentionDays {
    /// How many days to keep completed workflow executions
    pub workflow_executions: i32,

    /// How many days to keep execution logs
    pub execution_logs: i32,

    /// How many days to keep journal entries
    pub journal_entries: i32,

    /// How many days to keep completed task executions
    pub task_executions: i32,

    /// How many days to keep completed outbox queue tasks
    pub outbox_queue: i32,
}

impl Default for RetentionDays {
    fn default() -> Self {
        Self {
            workflow_executions: 90, // Keep 3 months of workflow data
            execution_logs: 30,      // Keep 1 month of logs
            journal_entries: 90,     // Keep 3 months of journal for audit
            task_executions: 90,     // Keep 3 months of task data
            outbox_queue: 7,         // Keep 1 week of completed outbox tasks
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = BackgroundJobsConfig::default();

        assert!(config.metrics_enabled);
        assert_eq!(config.metrics_interval_secs, 3600);

        assert!(config.retention_enabled);
        assert_eq!(config.retention_interval_secs, 86400);

        assert_eq!(config.retention_days.workflow_executions, 90);
        assert_eq!(config.retention_days.execution_logs, 30);
    }

    #[test]
    fn test_retention_days_default() {
        let retention = RetentionDays::default();

        assert_eq!(retention.workflow_executions, 90);
        assert_eq!(retention.execution_logs, 30);
        assert_eq!(retention.journal_entries, 90);
        assert_eq!(retention.task_executions, 90);
        assert_eq!(retention.outbox_queue, 7);
    }

    /// Prometheus accepts `[a-zA-Z_:][a-zA-Z0-9_:]*` and nothing else.
    ///
    /// Every name below was once dotted, and the two ways that failed are both
    /// quiet: with the Prometheus recorder installed the first emission panics
    /// the job's task, so the job runs once and never again; without it the
    /// emission is a no-op and the series never appears. A retention job that
    /// stopped after one run looks exactly like one that is working, right up
    /// until the tables are too big.
    #[test]
    fn every_job_metric_name_is_valid_for_prometheus() {
        fn valid(name: &str) -> bool {
            let mut chars = name.chars();
            let first_ok = chars
                .next()
                .is_some_and(|c| c.is_ascii_alphabetic() || c == '_' || c == ':');
            first_ok && chars.all(|c| c.is_ascii_alphanumeric() || c == '_' || c == ':')
        }

        let names = [
            super::data_retention::METRIC_RUNS,
            super::data_retention::METRIC_DURATION,
            super::data_retention::METRIC_ROWS_DELETED,
            super::data_retention::METRIC_ERRORS,
            super::metrics_aggregation::METRIC_RUNS,
            super::metrics_aggregation::METRIC_DURATION,
            super::metrics_aggregation::METRIC_TYPES_PROCESSED,
        ];

        for name in names {
            assert!(
                valid(name),
                "`{name}` is not a valid Prometheus metric name"
            );
        }
    }

    /// The rule the test above enforces, stated as its own case so a change to
    /// `valid` that accidentally accepts everything fails here rather than
    /// passing silently.
    #[test]
    fn the_metric_name_rule_rejects_what_it_should() {
        let names = [
            super::data_retention::METRIC_RUNS,
            super::metrics_aggregation::METRIC_RUNS,
        ];
        for name in names {
            assert!(
                !name.contains('.'),
                "`{name}` contains a dot, which Prometheus rejects"
            );
        }
    }
}
