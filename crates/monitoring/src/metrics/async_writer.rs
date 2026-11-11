//! Metrics for the batched async database writer.

use metrics::{counter, describe_counter, describe_gauge, describe_histogram, gauge, histogram};
use std::time::Duration;

pub mod names {
    pub const ENQUEUED_TOTAL: &str = "orcher_async_writer_enqueued_total";
    pub const FLUSHED_TOTAL: &str = "orcher_async_writer_flushed_total";
    pub const FLUSH_COUNT_TOTAL: &str = "orcher_async_writer_flush_count_total";
    pub const ERRORS_TOTAL: &str = "orcher_async_writer_errors_total";
    pub const DROPPED_TOTAL: &str = "orcher_async_writer_dropped_total";
    pub const BUFFER_DEPTH: &str = "orcher_async_writer_buffer_depth";
    pub const FLUSH_DURATION_SECONDS: &str = "orcher_async_writer_flush_duration_seconds";
    pub const FLUSH_BATCH_SIZE: &str = "orcher_async_writer_flush_batch_size";
}

/// Register metric descriptions. Call once at startup.
pub fn describe_async_writer_metrics() {
    describe_counter!(
        names::ENQUEUED_TOTAL,
        "Total journal entries enqueued for async write"
    );
    describe_counter!(
        names::FLUSHED_TOTAL,
        "Total journal entries successfully flushed to database"
    );
    describe_counter!(
        names::FLUSH_COUNT_TOTAL,
        "Total number of flush operations performed"
    );
    describe_counter!(
        names::ERRORS_TOTAL,
        "Total write errors encountered during flush"
    );
    describe_counter!(
        names::DROPPED_TOTAL,
        "Total entries dropped due to buffer overflow"
    );
    describe_gauge!(
        names::BUFFER_DEPTH,
        "Current number of entries in the async writer buffer"
    );
    describe_histogram!(
        names::FLUSH_DURATION_SECONDS,
        "Duration of flush operations in seconds"
    );
    describe_histogram!(
        names::FLUSH_BATCH_SIZE,
        "Number of entries written per flush operation"
    );
}

pub fn record_async_writer_enqueue(count: u64) {
    counter!(names::ENQUEUED_TOTAL).increment(count);
}

pub fn record_async_writer_flush(entry_count: u64, duration: Duration) {
    counter!(names::FLUSHED_TOTAL).increment(entry_count);
    counter!(names::FLUSH_COUNT_TOTAL).increment(1);
    histogram!(names::FLUSH_DURATION_SECONDS).record(duration.as_secs_f64());
    histogram!(names::FLUSH_BATCH_SIZE).record(entry_count as f64);
}

pub fn record_async_writer_error() {
    counter!(names::ERRORS_TOTAL).increment(1);
}

pub fn record_async_writer_dropped(count: u64) {
    counter!(names::DROPPED_TOTAL).increment(count);
}

pub fn update_async_writer_buffer_depth(depth: u64) {
    gauge!(names::BUFFER_DEPTH).set(depth as f64);
}

#[derive(Clone, Debug, Default)]
pub struct AsyncWriterStats {
    pub enqueued: u64,
    pub flushed: u64,
    pub flush_count: u64,
    pub errors: u64,
    pub dropped: u64,
    pub buffer_depth: u64,
}

impl AsyncWriterStats {
    pub fn flush_efficiency(&self) -> f64 {
        if self.enqueued == 0 {
            1.0
        } else {
            self.flushed as f64 / self.enqueued as f64
        }
    }

    pub fn drop_rate(&self) -> f64 {
        if self.enqueued == 0 {
            0.0
        } else {
            self.dropped as f64 / self.enqueued as f64
        }
    }

    pub fn error_rate(&self) -> f64 {
        if self.flush_count == 0 {
            0.0
        } else {
            self.errors as f64 / self.flush_count as f64
        }
    }

    pub fn average_batch_size(&self) -> f64 {
        if self.flush_count == 0 {
            0.0
        } else {
            self.flushed as f64 / self.flush_count as f64
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_stats_flush_efficiency() {
        let stats = AsyncWriterStats {
            enqueued: 100,
            flushed: 95,
            ..Default::default()
        };
        assert!((stats.flush_efficiency() - 0.95).abs() < 0.001);
    }

    #[test]
    fn test_stats_flush_efficiency_zero_enqueued() {
        let stats = AsyncWriterStats::default();
        assert!((stats.flush_efficiency() - 1.0).abs() < 0.001);
    }

    #[test]
    fn test_stats_drop_rate() {
        let stats = AsyncWriterStats {
            enqueued: 100,
            dropped: 5,
            ..Default::default()
        };
        assert!((stats.drop_rate() - 0.05).abs() < 0.001);
    }

    #[test]
    fn test_stats_error_rate() {
        let stats = AsyncWriterStats {
            flush_count: 100,
            errors: 2,
            ..Default::default()
        };
        assert!((stats.error_rate() - 0.02).abs() < 0.001);
    }

    #[test]
    fn test_stats_average_batch_size() {
        let stats = AsyncWriterStats {
            flushed: 1000,
            flush_count: 10,
            ..Default::default()
        };
        assert!((stats.average_batch_size() - 100.0).abs() < 0.001);
    }

    #[test]
    fn test_metric_names() {
        assert!(names::ENQUEUED_TOTAL.starts_with("orcher_async_writer_"));
        assert!(names::FLUSHED_TOTAL.starts_with("orcher_async_writer_"));
        assert!(names::FLUSH_COUNT_TOTAL.starts_with("orcher_async_writer_"));
        assert!(names::ERRORS_TOTAL.starts_with("orcher_async_writer_"));
        assert!(names::DROPPED_TOTAL.starts_with("orcher_async_writer_"));
        assert!(names::BUFFER_DEPTH.starts_with("orcher_async_writer_"));
        assert!(names::FLUSH_DURATION_SECONDS.starts_with("orcher_async_writer_"));
        assert!(names::FLUSH_BATCH_SIZE.starts_with("orcher_async_writer_"));
    }
}
