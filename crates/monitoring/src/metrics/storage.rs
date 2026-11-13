//! Storage and DB connection pool metrics.

use crate::error::Result;
use metrics::{counter, describe_counter, describe_gauge, describe_histogram, gauge, histogram};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{debug, warn};

#[derive(Debug, Clone, Default)]
pub struct DbPoolMetrics {
    pub active: u32,
    pub idle: u32,
    pub total: u32,
    pub max: u32,
}

impl DbPoolMetrics {
    pub fn new(active: u32, idle: u32, total: u32, max: u32) -> Self {
        Self {
            active,
            idle,
            total,
            max,
        }
    }

    pub fn utilization_ratio(&self) -> f64 {
        if self.max == 0 {
            0.0
        } else {
            self.active as f64 / self.max as f64
        }
    }

    /// >80% utilization.
    pub fn is_high_utilization(&self) -> bool {
        self.utilization_ratio() > 0.8
    }

    /// >90% utilization.
    pub fn is_critical(&self) -> bool {
        self.utilization_ratio() > 0.9
    }
}

/// Register metric descriptions. Call once at startup.
pub fn describe_db_pool_metrics() {
    use super::names::db_pool;

    describe_gauge!(
        db_pool::CONNECTIONS_ACTIVE,
        "Number of active (in-use) database connections"
    );
    describe_gauge!(
        db_pool::CONNECTIONS_IDLE,
        "Number of idle database connections available in the pool"
    );
    describe_gauge!(
        db_pool::CONNECTIONS_TOTAL,
        "Total number of connections currently in the pool"
    );
    describe_gauge!(
        db_pool::CONNECTIONS_MAX,
        "Maximum number of connections configured for the pool"
    );
    describe_gauge!(
        db_pool::UTILIZATION_RATIO,
        "Connection pool utilization ratio (active / max)"
    );
    describe_counter!(
        db_pool::ACQUISITIONS_TOTAL,
        "Total number of connection acquisitions from the pool"
    );
    describe_counter!(
        db_pool::ACQUISITION_TIMEOUTS_TOTAL,
        "Total number of failed connection acquisitions due to timeout"
    );
    describe_histogram!(
        db_pool::ACQUISITION_DURATION_SECONDS,
        "Time taken to acquire a connection from the pool"
    );
    describe_histogram!(
        db_pool::QUERY_DURATION_SECONDS,
        "Database query execution duration in seconds"
    );
    describe_counter!(
        db_pool::QUERIES_TOTAL,
        "Total number of database queries executed"
    );
    describe_counter!(
        db_pool::QUERY_ERRORS_TOTAL,
        "Total number of database query errors"
    );
    describe_gauge!(
        db_pool::HEALTH_STATUS,
        "Database health status (1 = healthy, 0 = unhealthy)"
    );
    describe_gauge!(
        db_pool::LAST_HEALTH_CHECK_SECONDS,
        "Seconds since last successful database health check"
    );
}

pub fn update_db_pool_metrics(metrics: &DbPoolMetrics) {
    use super::names::db_pool;

    gauge!(db_pool::CONNECTIONS_ACTIVE).set(metrics.active as f64);
    gauge!(db_pool::CONNECTIONS_IDLE).set(metrics.idle as f64);
    gauge!(db_pool::CONNECTIONS_TOTAL).set(metrics.total as f64);
    gauge!(db_pool::CONNECTIONS_MAX).set(metrics.max as f64);
    gauge!(db_pool::UTILIZATION_RATIO).set(metrics.utilization_ratio());

    // Log warning if pool is approaching exhaustion
    if metrics.is_critical() {
        warn!(
            active = metrics.active,
            max = metrics.max,
            utilization = %format!("{:.1}%", metrics.utilization_ratio() * 100.0),
            "Database connection pool critically low!"
        );
    } else if metrics.is_high_utilization() {
        debug!(
            active = metrics.active,
            max = metrics.max,
            utilization = %format!("{:.1}%", metrics.utilization_ratio() * 100.0),
            "Database connection pool utilization is high"
        );
    }
}

pub fn update_connection_metrics(active: u32, idle: u32, max: u32) {
    let total = active + idle;
    let metrics = DbPoolMetrics::new(active, idle, total, max);
    update_db_pool_metrics(&metrics);
}

pub fn record_connection_acquisition(duration: Duration) {
    use super::names::db_pool;

    counter!(db_pool::ACQUISITIONS_TOTAL).increment(1);
    histogram!(db_pool::ACQUISITION_DURATION_SECONDS).record(duration.as_secs_f64());
}

pub fn record_connection_timeout() {
    use super::names::db_pool;

    counter!(db_pool::ACQUISITION_TIMEOUTS_TOTAL).increment(1);
}

/// Sets gauge: 1.0 = healthy, 0.0 = unhealthy.
pub fn update_db_health_status(healthy: bool) {
    use super::names::db_pool;

    gauge!(db_pool::HEALTH_STATUS).set(if healthy { 1.0 } else { 0.0 });
}

pub fn update_last_health_check(seconds: u64) {
    use super::names::db_pool;

    gauge!(db_pool::LAST_HEALTH_CHECK_SECONDS).set(seconds as f64);
}

/// Wraps a query future with automatic duration, count, and error tracking.
pub async fn track_query_duration<F, T, E>(
    query_type: &str,
    table: &str,
    f: F,
) -> std::result::Result<T, E>
where
    F: std::future::Future<Output = std::result::Result<T, E>>,
{
    use super::names::db_pool;

    let start = Instant::now();
    let result = f.await;
    let duration = start.elapsed();

    // Record query count
    counter!(
        db_pool::QUERIES_TOTAL,
        "query_type" => query_type.to_string(),
        "table" => table.to_string()
    )
    .increment(1);

    // Record query duration
    histogram!(
        db_pool::QUERY_DURATION_SECONDS,
        "query_type" => query_type.to_string(),
        "table" => table.to_string()
    )
    .record(duration.as_secs_f64());

    // Track errors
    if result.is_err() {
        counter!(
            db_pool::QUERY_ERRORS_TOTAL,
            "query_type" => query_type.to_string(),
            "table" => table.to_string()
        )
        .increment(1);
    }

    result
}

/// Wraps a storage operation future with automatic duration and status tracking.
pub async fn track_storage_operation<F, T>(operation: &str, entity: &str, f: F) -> Result<T>
where
    F: std::future::Future<Output = Result<T>>,
{
    let start = Instant::now();
    let result = f.await;
    let duration = start.elapsed();

    // Increment operation counter
    counter!(
        super::names::storage::OPERATIONS_TOTAL,
        "operation" => operation.to_string(),
        "entity" => entity.to_string()
    )
    .increment(1);

    // Record operation duration
    histogram!(
        super::names::storage::OPERATION_DURATION_SECONDS,
        "operation" => operation.to_string()
    )
    .record(duration.as_secs_f64());

    // Track result status
    match &result {
        Ok(_) => {
            counter!(
                super::names::storage::OPERATIONS_TOTAL,
                "operation" => operation.to_string(),
                "result" => "success".to_string()
            )
            .increment(1);
        }
        Err(_) => {
            counter!(
                super::names::storage::OPERATIONS_TOTAL,
                "operation" => operation.to_string(),
                "result" => "failure".to_string()
            )
            .increment(1);
        }
    }

    result
}

pub fn update_cache_hit_ratio(hits: u64, misses: u64) {
    let total = hits.saturating_add(misses);
    let ratio = if total > 0 {
        hits as f64 / total as f64
    } else {
        0.0
    };
    gauge!(super::names::storage::CACHE_HIT_RATIO).set(ratio);
}

/// Legacy; prefer `update_db_pool_metrics` for comprehensive monitoring.
pub fn update_connection_pool_size(size: usize) {
    gauge!(super::names::storage::CONNECTION_POOL_SIZE).set(size as f64);
}

// ============================================================================
// Journal Cache Metrics
// ============================================================================

pub fn record_journal_cache_hit() {
    use super::names::journal_cache;
    counter!(journal_cache::CACHE_HITS_TOTAL).increment(1);
    counter!(journal_cache::DB_READS_AVOIDED).increment(1);
}

pub fn record_journal_cache_miss() {
    use super::names::journal_cache;
    counter!(journal_cache::CACHE_MISSES_TOTAL).increment(1);
}

pub fn record_journal_incremental_load(entries_loaded: usize) {
    use super::names::journal_cache;
    counter!(journal_cache::INCREMENTAL_LOADS_TOTAL).increment(1);
    counter!(journal_cache::INCREMENTAL_ENTRIES_LOADED).increment(entries_loaded as u64);
}

pub fn update_journal_cache_size(size: usize) {
    gauge!(super::names::journal_cache::CACHED_JOURNALS).set(size as f64);
}

/// Counts hits + incremental loads as effective hits (both avoid full DB loads).
pub fn update_journal_cache_hit_ratio(hits: u64, misses: u64, incremental_loads: u64) {
    let total = hits
        .saturating_add(misses)
        .saturating_add(incremental_loads);
    // Count hits + incremental loads as "effective hits" since they avoid full DB loads
    let effective_hits = hits.saturating_add(incremental_loads);
    let ratio = if total > 0 {
        effective_hits as f64 / total as f64
    } else {
        0.0
    };
    gauge!(super::names::journal_cache::HIT_RATIO).set(ratio);
}

/// Background collector for DB connection pool metrics.
#[derive(Clone)]
pub struct DbPoolMonitor {
    stats_fn: Arc<dyn Fn() -> DbPoolMetrics + Send + Sync>,
    last_health_check: Arc<AtomicU64>,
}

impl DbPoolMonitor {
    pub fn new<F>(stats_fn: F) -> Self
    where
        F: Fn() -> DbPoolMetrics + Send + Sync + 'static,
    {
        Self {
            stats_fn: Arc::new(stats_fn),
            last_health_check: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Convenient constructor for sqlx PgPool-style APIs.
    pub fn from_pool_fns<S, I>(size_fn: S, idle_fn: I, max_connections: u32) -> Self
    where
        S: Fn() -> u32 + Send + Sync + 'static,
        I: Fn() -> u32 + Send + Sync + 'static,
    {
        Self::new(move || {
            let total = size_fn();
            let idle = idle_fn();
            let active = total.saturating_sub(idle);
            DbPoolMetrics::new(active, idle, total, max_connections)
        })
    }

    pub fn collect(&self) {
        let metrics = (self.stats_fn)();
        update_db_pool_metrics(&metrics);
    }

    pub fn record_health_check(&self, healthy: bool) {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        if healthy {
            self.last_health_check.store(now, Ordering::Relaxed);
        }
        update_db_health_status(healthy);
    }

    pub fn seconds_since_last_health_check(&self) -> u64 {
        let last = self.last_health_check.load(Ordering::Relaxed);
        if last == 0 {
            return 0; // Never checked
        }

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        now.saturating_sub(last)
    }

    /// Spawns a background task. Call `.abort()` on the handle to stop.
    pub fn start(self, interval: Duration) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);

            loop {
                ticker.tick().await;

                // Collect pool metrics
                self.collect();

                // Update health check age
                let health_age = self.seconds_since_last_health_check();
                update_last_health_check(health_age);

                debug!(
                    health_age_secs = health_age,
                    "Database pool metrics collected"
                );
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_track_storage_operation_success() {
        let result = track_storage_operation("read", "task", async { Ok("success") }).await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "success");
    }

    #[tokio::test]
    async fn test_track_storage_operation_failure() {
        let result: Result<&str> = track_storage_operation("write", "workflow", async {
            Err(crate::error::MonitoringError::metric_collection_error(
                "storage_operation",
                "test error",
            ))
        })
        .await;

        assert!(result.is_err());
    }

    #[test]
    fn test_update_cache_hit_ratio() {
        update_cache_hit_ratio(80, 20); // 80% hit ratio
        update_cache_hit_ratio(50, 50); // 50% hit ratio
        update_cache_hit_ratio(0, 100); // 0% hit ratio
    }

    #[test]
    fn test_update_cache_hit_ratio_edge_cases() {
        update_cache_hit_ratio(0, 0); // No operations
        update_cache_hit_ratio(100, 0); // 100% hit ratio
        update_cache_hit_ratio(u64::MAX, 1); // Very large numbers
    }

    #[test]
    fn test_update_connection_pool_size() {
        update_connection_pool_size(10);
        update_connection_pool_size(0);
        update_connection_pool_size(100);
    }

    #[test]
    fn test_db_pool_metrics() {
        let metrics = DbPoolMetrics::new(5, 3, 8, 20);

        assert_eq!(metrics.active, 5);
        assert_eq!(metrics.idle, 3);
        assert_eq!(metrics.total, 8);
        assert_eq!(metrics.max, 20);
        assert!((metrics.utilization_ratio() - 0.25).abs() < 0.001);
        assert!(!metrics.is_high_utilization());
        assert!(!metrics.is_critical());
    }

    #[test]
    fn test_db_pool_metrics_high_utilization() {
        let metrics = DbPoolMetrics::new(17, 1, 18, 20);

        assert!(metrics.is_high_utilization());
        assert!(!metrics.is_critical());
    }

    #[test]
    fn test_db_pool_metrics_critical() {
        let metrics = DbPoolMetrics::new(19, 0, 19, 20);

        assert!(metrics.is_high_utilization());
        assert!(metrics.is_critical());
    }

    #[test]
    fn test_db_pool_metrics_zero_max() {
        let metrics = DbPoolMetrics::new(0, 0, 0, 0);

        assert_eq!(metrics.utilization_ratio(), 0.0);
        assert!(!metrics.is_high_utilization());
        assert!(!metrics.is_critical());
    }

    #[test]
    fn test_update_db_pool_metrics() {
        let metrics = DbPoolMetrics::new(5, 3, 8, 20);
        update_db_pool_metrics(&metrics);
        // Metrics should be recorded without panic
    }

    #[test]
    fn test_update_connection_metrics() {
        update_connection_metrics(5, 3, 20);
        // Should update all metrics without panic
    }

    #[test]
    fn test_record_connection_acquisition() {
        record_connection_acquisition(Duration::from_millis(50));
        // Should record without panic
    }

    #[test]
    fn test_record_connection_timeout() {
        record_connection_timeout();
        // Should record without panic
    }

    #[test]
    fn test_update_db_health_status() {
        update_db_health_status(true);
        update_db_health_status(false);
        // Should update without panic
    }

    #[tokio::test]
    async fn test_track_query_duration_success() {
        let result: std::result::Result<&str, &str> =
            track_query_duration("select", "workflow_executions", async { Ok("data") }).await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "data");
    }

    #[tokio::test]
    async fn test_track_query_duration_error() {
        let result: std::result::Result<&str, &str> =
            track_query_duration("insert", "workflow_executions", async { Err("db error") }).await;

        assert!(result.is_err());
    }

    #[test]
    fn test_db_pool_monitor_creation() {
        let monitor = DbPoolMonitor::new(|| DbPoolMetrics::new(5, 3, 8, 20));
        monitor.collect();
        // Should collect without panic
    }

    #[test]
    fn test_db_pool_monitor_from_fns() {
        let monitor = DbPoolMonitor::from_pool_fns(
            || 8, // size
            || 3, // idle
            20,   // max
        );
        monitor.collect();
        // Should collect without panic
    }

    #[test]
    fn test_db_pool_monitor_health_check() {
        let monitor = DbPoolMonitor::new(DbPoolMetrics::default);

        assert_eq!(monitor.seconds_since_last_health_check(), 0);

        monitor.record_health_check(true);
        // After recording, should have a recent timestamp
        assert!(monitor.seconds_since_last_health_check() < 2);
    }
}
