//! Event system metrics (publishing, persistence, latency).

use metrics::{counter, histogram};
use std::time::Duration;

pub fn track_event_published(event_type: &str, status: &str) {
    counter!(super::names::events::PUBLISHED_TOTAL, "event_type" => event_type.to_string(), "status" => status.to_string()).increment(1);
}

pub fn track_event_persisted(event_type: &str) {
    counter!(super::names::events::PERSISTED_TOTAL, "event_type" => event_type.to_string())
        .increment(1);
}

pub fn track_event_persistence_error(error_type: &str) {
    counter!(super::names::events::PERSISTENCE_ERRORS_TOTAL, "error_type" => error_type.to_string()).increment(1);
}

pub fn record_event_publish_latency(duration: Duration) {
    histogram!(super::names::events::PUBLISH_LATENCY_SECONDS).record(duration.as_secs_f64());
}

pub fn record_event_persistence_latency(duration: Duration) {
    histogram!(super::names::events::PERSISTENCE_LATENCY_SECONDS).record(duration.as_secs_f64());
}

/// Combines event publish tracking with latency recording.
pub fn track_event_published_with_latency(event_type: &str, status: &str, duration: Duration) {
    track_event_published(event_type, status);
    record_event_publish_latency(duration);
}

/// Combines event persistence tracking with latency recording.
pub fn track_event_persisted_with_latency(event_type: &str, duration: Duration) {
    track_event_persisted(event_type);
    record_event_persistence_latency(duration);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_track_event_published() {
        track_event_published("task.completed", "success");
        track_event_published("workflow.started", "success");
        track_event_published("task.failed", "failed");
    }

    #[test]
    fn test_track_event_persisted() {
        track_event_persisted("task.completed");
        track_event_persisted("workflow.started");
    }

    #[test]
    fn test_track_event_persistence_error() {
        track_event_persistence_error("database_error");
        track_event_persistence_error("timeout");
        track_event_persistence_error("serialization_error");
    }

    #[test]
    fn test_record_event_publish_latency() {
        record_event_publish_latency(Duration::from_millis(5));
        record_event_publish_latency(Duration::from_micros(500));
    }

    #[test]
    fn test_record_event_persistence_latency() {
        record_event_persistence_latency(Duration::from_millis(25));
        record_event_persistence_latency(Duration::from_millis(100));
    }

    #[test]
    fn test_track_event_published_with_latency() {
        track_event_published_with_latency("task.completed", "success", Duration::from_millis(5));
    }

    #[test]
    fn test_track_event_persisted_with_latency() {
        track_event_persisted_with_latency("workflow.started", Duration::from_millis(25));
    }
}
