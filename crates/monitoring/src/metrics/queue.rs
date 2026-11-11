//! Queue metrics (dispatch, consumption, DLQ, health).

use metrics::{counter, gauge, histogram};

pub fn track_task_dispatched(_queue_name: &str, _status: &str, duration_ms: u64) {
    counter!("orcher_queue_tasks_dispatched_total").increment(1);
    histogram!("orcher_queue_dispatch_duration_ms").record(duration_ms as f64);
}

pub fn track_event_consumed(_queue_name: &str, _status: &str, duration_ms: u64) {
    counter!("orcher_queue_events_consumed_total").increment(1);
    histogram!("orcher_queue_processing_duration_ms").record(duration_ms as f64);
}

pub fn track_retry_attempt(_queue_name: &str, _retry_count: u32) {
    counter!("orcher_queue_retries_total").increment(1);
}

pub fn track_dlq_message(_queue_name: &str, _reason: &str) {
    counter!("orcher_queue_dlq_messages_total").increment(1);
}

pub fn update_queue_depth(_queue_name: &str, depth: u64) {
    gauge!("orcher_queue_depth").set(depth as f64);
}

pub fn update_consumer_lag(_queue_name: &str, lag: u64) {
    gauge!("orcher_queue_consumer_lag").set(lag as f64);
}

/// Sets queue health: 1.0 = healthy, 0.0 = unhealthy.
pub fn update_queue_health(_queue_name: &str, healthy: bool) {
    gauge!("orcher_queue_health").set(if healthy { 1.0 } else { 0.0 });
}

pub fn track_health_check(_queue_name: &str, duration_ms: u64) {
    histogram!("orcher_queue_health_check_duration_ms").record(duration_ms as f64);
}

pub fn update_connection_pool(_provider: &str, active: u32, idle: u32, max: u32) {
    gauge!("orcher_queue_connection_pool_active").set(active as f64);
    gauge!("orcher_queue_connection_pool_idle").set(idle as f64);

    if max > 0 {
        let utilization = (active as f64 / max as f64) * 100.0;
        gauge!("orcher_queue_connection_pool_utilization_percent").set(utilization);
    }
}

pub fn track_event_type(_event_type: &str, _status: &str) {
    counter!("orcher_queue_event_type_total").increment(1);
}

pub fn track_dispatcher_operation(_success: bool) {
    counter!("orcher_queue_dispatcher_operations_total").increment(1);
}

pub fn track_consumer_operation(_success: bool) {
    counter!("orcher_queue_consumer_operations_total").increment(1);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_track_task_dispatched() {
        track_task_dispatched("test-queue", "success", 100);
        track_task_dispatched("test-queue", "failed", 50);
    }

    #[test]
    fn test_track_event_consumed() {
        track_event_consumed("test-queue", "success", 25);
        track_event_consumed("test-queue", "retry", 15);
    }

    #[test]
    fn test_update_queue_metrics() {
        update_queue_depth("test-queue", 42);
        update_consumer_lag("test-queue", 10);
        update_queue_health("test-queue", true);
        update_queue_health("test-queue", false);
    }

    #[test]
    fn test_track_retry_and_dlq() {
        track_retry_attempt("test-queue", 2);
        track_dlq_message("test-queue", "max_retries_exceeded");
    }

    #[test]
    fn test_connection_pool_metrics() {
        update_connection_pool("redis", 15, 5, 20);
        update_connection_pool("rabbitmq", 0, 10, 10);
    }

    #[test]
    fn test_event_type_tracking() {
        track_event_type("ExecutionStarted", "success");
        track_event_type("ExecutionCompleted", "success");
        track_event_type("ExecutionFailed", "retry");
    }

    #[test]
    fn test_operation_tracking() {
        track_dispatcher_operation(true);
        track_dispatcher_operation(false);
        track_consumer_operation(true);
        track_consumer_operation(false);
    }

    #[test]
    fn test_message_tracking() {
        use std::time::Duration;
        track_queue_message_published("test-queue", "success");
        track_queue_message_consumed("test-queue", "success");
        track_queue_publish_error("test-queue", "timeout");
        track_queue_consume_error("test-queue", "parse_error");
        record_queue_publish_latency("test-queue", Duration::from_millis(10));
    }
}

pub fn track_queue_message_published(queue_name: &str, status: &str) {
    counter!(super::names::queue::MESSAGES_PUBLISHED_TOTAL, "queue" => queue_name.to_string(), "status" => status.to_string()).increment(1);
}

pub fn track_queue_message_consumed(queue_name: &str, status: &str) {
    counter!(super::names::queue::MESSAGES_CONSUMED_TOTAL, "queue" => queue_name.to_string(), "status" => status.to_string()).increment(1);
}

pub fn track_queue_publish_error(queue_name: &str, error_type: &str) {
    counter!(super::names::queue::PUBLISH_ERRORS_TOTAL, "queue" => queue_name.to_string(), "error_type" => error_type.to_string()).increment(1);
}

pub fn track_queue_consume_error(queue_name: &str, error_type: &str) {
    counter!(super::names::queue::CONSUME_ERRORS_TOTAL, "queue" => queue_name.to_string(), "error_type" => error_type.to_string()).increment(1);
}

pub fn record_queue_publish_latency(queue_name: &str, duration: std::time::Duration) {
    histogram!(super::names::queue::PUBLISH_LATENCY_SECONDS, "queue" => queue_name.to_string())
        .record(duration.as_secs_f64());
}
