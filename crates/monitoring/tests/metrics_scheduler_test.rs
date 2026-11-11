//! Tests for scheduler metrics functionality
//!
//! This module tests scheduler metrics tracking including queue size,
//! trigger events, and scheduling timing information.

use orcher_monitoring::metrics::scheduler::{
    track_trigger_fired, update_next_fire_time, update_scheduler_queue_size,
};

#[test]
fn test_update_scheduler_queue_size() {
    // Test updating queue size with various values
    update_scheduler_queue_size(10);
    update_scheduler_queue_size(0);
    update_scheduler_queue_size(100);
    update_scheduler_queue_size(1000);
}

#[test]
fn test_track_trigger_fired() {
    // Test tracking different trigger types
    track_trigger_fired("cron");
    track_trigger_fired("interval");
    track_trigger_fired("manual");
    track_trigger_fired("webhook");
}

#[test]
fn test_track_trigger_fired_with_same_type() {
    // Test multiple triggers of the same type
    let trigger_type = "cron";
    track_trigger_fired(trigger_type);
    track_trigger_fired(trigger_type);
    track_trigger_fired(trigger_type);
}

#[test]
fn test_update_next_fire_time() {
    // Test updating next fire time with various timestamps
    let now = orcher_common::types::time::Timestamptz::now().timestamp();

    update_next_fire_time(now);
    update_next_fire_time(now + 3600); // 1 hour from now
    update_next_fire_time(now + 86400); // 1 day from now
}

#[test]
fn test_update_next_fire_time_edge_cases() {
    // Test edge cases
    update_next_fire_time(0); // Unix epoch
    update_next_fire_time(-1); // Before epoch (should handle gracefully)
    update_next_fire_time(i64::MAX); // Far future
}

#[test]
fn test_scheduler_metrics_workflow() {
    // Test a typical scheduler workflow

    // Start with empty queue
    update_scheduler_queue_size(0);

    // Add some items to queue
    update_scheduler_queue_size(5);

    // Fire a trigger
    track_trigger_fired("cron");

    // Update next fire time
    let next_fire = orcher_common::types::time::Timestamptz::now().timestamp() + 300; // 5 minutes
    update_next_fire_time(next_fire);

    // Process some items
    update_scheduler_queue_size(3);

    // Fire another trigger
    track_trigger_fired("interval");
}

#[test]
fn test_multiple_trigger_types_concurrently() {
    // Test tracking multiple different trigger types
    let trigger_types = vec!["cron", "interval", "manual", "webhook", "event"];

    for trigger_type in trigger_types {
        track_trigger_fired(trigger_type);
    }
}
