//! Atomic metrics tracking for EventStore operations.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

/// Thread-safe metrics for EventStore operations using atomics.
pub struct EventStoreMetrics {
    pub append_count: AtomicU64,
    pub read_count: AtomicU64,
    pub query_count: AtomicU64,
    pub error_count: AtomicU64,
    /// Exponential moving average in microseconds.
    pub append_latency_us: AtomicU64,
    /// Exponential moving average in microseconds.
    pub read_latency_us: AtomicU64,
    pub active_connections: AtomicU64,
    pub total_events: AtomicU64,
}

impl EventStoreMetrics {
    pub fn new() -> Self {
        Self {
            append_count: AtomicU64::new(0),
            read_count: AtomicU64::new(0),
            query_count: AtomicU64::new(0),
            error_count: AtomicU64::new(0),
            append_latency_us: AtomicU64::new(0),
            read_latency_us: AtomicU64::new(0),
            active_connections: AtomicU64::new(0),
            total_events: AtomicU64::new(0),
        }
    }

    pub fn record_append(&self, duration: Duration) {
        self.append_count.fetch_add(1, Ordering::Relaxed);
        self.update_latency(&self.append_latency_us, duration);
    }

    pub fn record_read(&self, duration: Duration) {
        self.read_count.fetch_add(1, Ordering::Relaxed);
        self.update_latency(&self.read_latency_us, duration);
    }

    pub fn record_query(&self) {
        self.query_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_error(&self) {
        self.error_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn set_total_events(&self, count: u64) {
        self.total_events.store(count, Ordering::Relaxed);
    }

    pub fn set_active_connections(&self, count: u64) {
        self.active_connections.store(count, Ordering::Relaxed);
    }

    /// EMA with 90/10 weighting: 90% old average, 10% new sample.
    fn update_latency(&self, latency: &AtomicU64, duration: Duration) {
        let new_latency = duration.as_micros() as u64;
        let old_latency = latency.load(Ordering::Relaxed);

        // Exponential moving average: (old * 9 + new) / 10
        let avg = if old_latency == 0 {
            new_latency
        } else {
            (old_latency * 9 + new_latency) / 10
        };

        latency.store(avg, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            append_count: self.append_count.load(Ordering::Relaxed),
            read_count: self.read_count.load(Ordering::Relaxed),
            query_count: self.query_count.load(Ordering::Relaxed),
            error_count: self.error_count.load(Ordering::Relaxed),
            append_latency_us: self.append_latency_us.load(Ordering::Relaxed),
            read_latency_us: self.read_latency_us.load(Ordering::Relaxed),
            active_connections: self.active_connections.load(Ordering::Relaxed),
            total_events: self.total_events.load(Ordering::Relaxed),
        }
    }

    /// Resets operation counters and latencies but not state metrics (connections, total events).
    pub fn reset(&self) {
        self.append_count.store(0, Ordering::Relaxed);
        self.read_count.store(0, Ordering::Relaxed);
        self.query_count.store(0, Ordering::Relaxed);
        self.error_count.store(0, Ordering::Relaxed);
        self.append_latency_us.store(0, Ordering::Relaxed);
        self.read_latency_us.store(0, Ordering::Relaxed);
        // Don't reset active_connections or total_events as they're state, not counters
    }
}

impl Default for EventStoreMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// Point-in-time copy of all metric values.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MetricsSnapshot {
    pub append_count: u64,
    pub read_count: u64,
    pub query_count: u64,
    pub error_count: u64,
    pub append_latency_us: u64,
    pub read_latency_us: u64,
    pub active_connections: u64,
    pub total_events: u64,
}

impl MetricsSnapshot {
    pub fn total_operations(&self) -> u64 {
        self.append_count + self.read_count + self.query_count
    }

    pub fn error_rate(&self) -> f64 {
        let total = self.total_operations();
        if total == 0 {
            0.0
        } else {
            (self.error_count as f64 / total as f64) * 100.0
        }
    }

    pub fn success_rate(&self) -> f64 {
        100.0 - self.error_rate()
    }

    pub fn append_latency_ms(&self) -> f64 {
        self.append_latency_us as f64 / 1000.0
    }

    pub fn read_latency_ms(&self) -> f64 {
        self.read_latency_us as f64 / 1000.0
    }

    pub fn format(&self) -> String {
        format!(
            "EventStore Metrics:\n\
             Operations: {} appends, {} reads, {} queries\n\
             Latencies: {:.2}ms append, {:.2}ms read\n\
             Errors: {} ({:.2}% error rate)\n\
             Resources: {} connections, {} total events",
            self.append_count,
            self.read_count,
            self.query_count,
            self.append_latency_ms(),
            self.read_latency_ms(),
            self.error_count,
            self.error_rate(),
            self.active_connections,
            self.total_events
        )
    }
}

impl std::fmt::Display for MetricsSnapshot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.format())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_metrics_initialization() {
        let metrics = EventStoreMetrics::new();
        let snapshot = metrics.snapshot();

        assert_eq!(snapshot.append_count, 0);
        assert_eq!(snapshot.read_count, 0);
        assert_eq!(snapshot.query_count, 0);
        assert_eq!(snapshot.error_count, 0);
        assert_eq!(snapshot.append_latency_us, 0);
        assert_eq!(snapshot.read_latency_us, 0);
    }

    #[test]
    fn test_record_append() {
        let metrics = EventStoreMetrics::new();

        metrics.record_append(Duration::from_micros(100));
        metrics.record_append(Duration::from_micros(200));

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.append_count, 2);
        assert!(snapshot.append_latency_us > 0);
    }

    #[test]
    fn test_record_read() {
        let metrics = EventStoreMetrics::new();

        metrics.record_read(Duration::from_micros(50));
        metrics.record_read(Duration::from_micros(150));

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.read_count, 2);
        assert!(snapshot.read_latency_us > 0);
    }

    #[test]
    fn test_record_error() {
        let metrics = EventStoreMetrics::new();

        metrics.record_error();
        metrics.record_error();

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.error_count, 2);
    }

    #[test]
    fn test_latency_moving_average() {
        let metrics = EventStoreMetrics::new();

        // First append: 100μs
        metrics.record_append(Duration::from_micros(100));
        let snapshot1 = metrics.snapshot();
        assert_eq!(snapshot1.append_latency_us, 100);

        // Second append: 200μs
        // EMA = (100 * 9 + 200) / 10 = 110
        metrics.record_append(Duration::from_micros(200));
        let snapshot2 = metrics.snapshot();
        assert_eq!(snapshot2.append_latency_us, 110);

        // Third append: 300μs
        // EMA = (110 * 9 + 300) / 10 = 129
        metrics.record_append(Duration::from_micros(300));
        let snapshot3 = metrics.snapshot();
        assert_eq!(snapshot3.append_latency_us, 129);
    }

    #[test]
    fn test_set_counters() {
        let metrics = EventStoreMetrics::new();

        metrics.set_total_events(1000);
        metrics.set_active_connections(5);

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.total_events, 1000);
        assert_eq!(snapshot.active_connections, 5);
    }

    #[test]
    fn test_snapshot_calculations() {
        let snapshot = MetricsSnapshot {
            append_count: 100,
            read_count: 50,
            query_count: 25,
            error_count: 5,
            append_latency_us: 1000,
            read_latency_us: 500,
            active_connections: 10,
            total_events: 5000,
        };

        assert_eq!(snapshot.total_operations(), 175);
        assert!((snapshot.error_rate() - 2.857).abs() < 0.01);
        assert!((snapshot.success_rate() - 97.143).abs() < 0.01);
        assert_eq!(snapshot.append_latency_ms(), 1.0);
        assert_eq!(snapshot.read_latency_ms(), 0.5);
    }

    #[test]
    fn test_error_rate_with_no_operations() {
        let snapshot = MetricsSnapshot {
            append_count: 0,
            read_count: 0,
            query_count: 0,
            error_count: 0,
            append_latency_us: 0,
            read_latency_us: 0,
            active_connections: 0,
            total_events: 0,
        };

        assert_eq!(snapshot.error_rate(), 0.0);
        assert_eq!(snapshot.success_rate(), 100.0);
    }

    #[test]
    fn test_metrics_reset() {
        let metrics = EventStoreMetrics::new();

        metrics.record_append(Duration::from_micros(100));
        metrics.record_read(Duration::from_micros(50));
        metrics.record_error();
        metrics.set_total_events(1000);
        metrics.set_active_connections(5);

        metrics.reset();

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.append_count, 0);
        assert_eq!(snapshot.read_count, 0);
        assert_eq!(snapshot.error_count, 0);
        assert_eq!(snapshot.append_latency_us, 0);
        assert_eq!(snapshot.read_latency_us, 0);
        // State metrics shouldn't be reset
        assert_eq!(snapshot.total_events, 1000);
        assert_eq!(snapshot.active_connections, 5);
    }

    #[test]
    fn test_metrics_format() {
        let snapshot = MetricsSnapshot {
            append_count: 100,
            read_count: 50,
            query_count: 25,
            error_count: 5,
            append_latency_us: 1500,
            read_latency_us: 800,
            active_connections: 10,
            total_events: 5000,
        };

        let formatted = snapshot.format();
        assert!(formatted.contains("100 appends"));
        assert!(formatted.contains("50 reads"));
        assert!(formatted.contains("25 queries"));
        assert!(formatted.contains("1.50ms append"));
        assert!(formatted.contains("0.80ms read"));
        assert!(formatted.contains("5 ("));
        assert!(formatted.contains("10 connections"));
        assert!(formatted.contains("5000 total events"));
    }

    #[test]
    fn test_concurrent_updates() {
        use std::sync::Arc;
        use std::thread;

        let metrics = Arc::new(EventStoreMetrics::new());
        let mut handles = vec![];

        // Spawn 10 threads, each recording 100 operations
        for _ in 0..10 {
            let metrics_clone = metrics.clone();
            let handle = thread::spawn(move || {
                for _ in 0..100 {
                    metrics_clone.record_append(Duration::from_micros(100));
                }
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.append_count, 1000);
    }
}
