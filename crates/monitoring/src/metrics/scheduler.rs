//! Scheduler metrics.

use metrics::{counter, gauge};

pub fn update_scheduler_queue_size(size: usize) {
    gauge!(super::names::scheduler::QUEUE_SIZE).set(size as f64);
}

pub fn track_trigger_fired(trigger_type: &str) {
    counter!(
        super::names::scheduler::TRIGGERS_FIRED_TOTAL,
        "trigger_type" => trigger_type.to_string()
    )
    .increment(1);
}

/// Unix timestamp of the next scheduled execution.
pub fn update_next_fire_time(timestamp: i64) {
    gauge!(super::names::scheduler::NEXT_FIRE_TIME).set(timestamp as f64);
}
