//! System resource metrics (CPU, memory). Requires `system-metrics` feature.

#[cfg(feature = "system-metrics")]
use metrics::gauge;
#[cfg(feature = "system-metrics")]
use std::sync::Arc;
#[cfg(feature = "system-metrics")]
use sysinfo::{CpuExt, System, SystemExt};
#[cfg(feature = "system-metrics")]
use tokio::sync::RwLock;
#[cfg(feature = "system-metrics")]
use tokio::time::Duration;

#[cfg(feature = "system-metrics")]
pub fn collect_cpu_metrics() {
    let mut sys = System::new_all();
    sys.refresh_cpu();

    let cpu_usage = sys.global_cpu_info().cpu_usage();
    gauge!(super::names::system::CPU_USAGE).set(cpu_usage as f64);
}

#[cfg(feature = "system-metrics")]
pub fn collect_memory_metrics() {
    let mut sys = System::new_all();
    sys.refresh_memory();

    let total = sys.total_memory();
    let used = sys.used_memory();
    let usage_percent = if total > 0 {
        (used as f64 / total as f64) * 100.0
    } else {
        0.0
    };

    gauge!(super::names::system::MEMORY_USAGE).set(usage_percent);
}

/// Spawns a background task collecting CPU/memory metrics at the given interval.
#[cfg(feature = "system-metrics")]
pub fn start_system_metrics_collection(interval: Duration) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval_timer = tokio::time::interval(interval);
        loop {
            interval_timer.tick().await;
            collect_cpu_metrics();
            collect_memory_metrics();
        }
    })
}

/// Cached system metrics collector with configurable refresh rate.
#[cfg(feature = "system-metrics")]
pub struct SystemMetricsCollector {
    system: Arc<RwLock<System>>,
    last_refresh: Arc<RwLock<std::time::Instant>>,
    refresh_interval: Duration,
}

#[cfg(feature = "system-metrics")]
impl SystemMetricsCollector {
    pub fn new(refresh_interval: Duration) -> Self {
        Self {
            system: Arc::new(RwLock::new(System::new_all())),
            last_refresh: Arc::new(RwLock::new(std::time::Instant::now())),
            refresh_interval,
        }
    }

    async fn ensure_fresh(&self) {
        let now = std::time::Instant::now();
        let last_refresh = *self.last_refresh.read().await;

        if now.duration_since(last_refresh) >= self.refresh_interval {
            let mut system = self.system.write().await;
            let mut last_refresh_write = self.last_refresh.write().await;

            system.refresh_all();
            *last_refresh_write = now;
        }
    }

    pub async fn collect_cpu_metrics(&self) {
        self.ensure_fresh().await;
        let system = self.system.read().await;
        let cpu_usage = system.global_cpu_info().cpu_usage();
        gauge!(super::names::system::CPU_USAGE).set(cpu_usage as f64);
    }

    pub async fn collect_memory_metrics(&self) {
        self.ensure_fresh().await;
        let system = self.system.read().await;

        let total = system.total_memory();
        let used = system.used_memory();
        let usage_percent = if total > 0 {
            (used as f64 / total as f64) * 100.0
        } else {
            0.0
        };

        gauge!(super::names::system::MEMORY_USAGE).set(usage_percent);
    }

    pub async fn collect_all_metrics(&self) {
        self.collect_cpu_metrics().await;
        self.collect_memory_metrics().await;
    }

    pub fn start_automatic_collection(&self, interval: Duration) -> tokio::task::JoinHandle<()> {
        let collector = SystemMetricsCollector {
            system: self.system.clone(),
            last_refresh: self.last_refresh.clone(),
            refresh_interval: self.refresh_interval,
        };

        tokio::spawn(async move {
            let mut interval_timer = tokio::time::interval(interval);
            loop {
                interval_timer.tick().await;
                collector.collect_all_metrics().await;
            }
        })
    }
}

// Provide no-op implementations when the feature is not enabled
#[cfg(not(feature = "system-metrics"))]
mod no_system_metrics {
    pub fn collect_cpu_metrics() {
        // No-op when feature is disabled
    }

    pub fn collect_memory_metrics() {
        // No-op when feature is disabled
    }

    pub fn start_system_metrics_collection(
        _interval: std::time::Duration,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async {
            // No-op task that does nothing
        })
    }
}

#[cfg(not(feature = "system-metrics"))]
pub use no_system_metrics::*;

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(feature = "system-metrics")]
    #[test]
    fn test_collect_cpu_metrics() {
        collect_cpu_metrics();
    }

    #[cfg(feature = "system-metrics")]
    #[test]
    fn test_collect_memory_metrics() {
        collect_memory_metrics();
    }

    #[cfg(feature = "system-metrics")]
    #[tokio::test]
    async fn test_system_metrics_collector() {
        let collector = SystemMetricsCollector::new(std::time::Duration::from_millis(100));

        collector.collect_cpu_metrics().await;
        collector.collect_memory_metrics().await;
        collector.collect_all_metrics().await;
    }

    #[cfg(feature = "system-metrics")]
    #[tokio::test]
    async fn test_automatic_collection() {
        let collector = SystemMetricsCollector::new(std::time::Duration::from_millis(10));
        let handle = collector.start_automatic_collection(std::time::Duration::from_millis(50));

        // Let it run for a bit
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        // Stop the collection
        handle.abort();
    }

    #[cfg(feature = "system-metrics")]
    #[test]
    fn test_start_system_metrics_collection() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let handle = start_system_metrics_collection(std::time::Duration::from_millis(10));

            // Let it run for a short time
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;

            // Stop collection
            handle.abort();
        });
    }

    #[cfg(not(feature = "system-metrics"))]
    #[test]
    fn test_no_op_functions() {
        // These should not panic when feature is disabled
        collect_cpu_metrics();
        collect_memory_metrics();

        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let handle = start_system_metrics_collection(std::time::Duration::from_millis(10));
            handle.abort();
        });
    }

    #[test]
    fn test_feature_conditional_compilation() {
        // This test ensures the code compiles regardless of feature flags
        #[cfg(feature = "system-metrics")]
        {
            collect_cpu_metrics();
            collect_memory_metrics();
        }

        #[cfg(not(feature = "system-metrics"))]
        {
            collect_cpu_metrics();
            collect_memory_metrics();
        }
    }
}
