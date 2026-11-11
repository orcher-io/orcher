//! Worker pool metrics.

use metrics::gauge;

pub fn update_worker_pool_size(size: usize) {
    gauge!(super::names::workers::POOL_SIZE).set(size as f64);
}

pub fn update_worker_counts(active_count: usize, idle_count: usize) {
    gauge!(super::names::workers::ACTIVE_COUNT).set(active_count as f64);
    gauge!(super::names::workers::IDLE_COUNT).set(idle_count as f64);
}

pub fn calculate_worker_utilization(active: usize, total: usize) {
    let utilization = if total > 0 {
        (active as f64 / total as f64) * 100.0
    } else {
        0.0
    };
    gauge!(super::names::workers::UTILIZATION).set(utilization);
}
