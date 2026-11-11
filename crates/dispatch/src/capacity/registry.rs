//! Capacity registry for tracking worker capacity across task queues

use super::TaskPermits;
use dashmap::DashMap;
use orcher_common::types::dispatch::{GlobalCapacityStats, QueueCapacityStats, TaskPermitStats};
use std::sync::Arc;
use std::time::Duration;
use tracing::{info, warn};

/// Tracks worker permits across all task queues.
pub struct CapacityRegistry {
    /// (task_queue, namespace) -> worker_identity -> permits
    workers: DashMap<(String, String), DashMap<String, Arc<TaskPermits>>>,
    default_capacity: usize,
    idle_timeout: Duration,
}

impl CapacityRegistry {
    pub fn new() -> Self {
        Self::with_config(100, Duration::from_secs(300))
    }

    pub fn with_config(default_capacity: usize, idle_timeout: Duration) -> Self {
        info!(
            default_capacity = default_capacity,
            idle_timeout_secs = idle_timeout.as_secs(),
            "Creating CapacityRegistry"
        );

        Self {
            workers: DashMap::new(),
            default_capacity,
            idle_timeout,
        }
    }

    /// Returns existing permits if the worker is already registered.
    pub fn register(
        &self,
        task_queue: &str,
        namespace: &str,
        identity: &str,
        max_permits: usize,
    ) -> Arc<TaskPermits> {
        let key = (task_queue.to_string(), namespace.to_string());
        let effective_permits = if max_permits == 0 {
            self.default_capacity
        } else {
            max_permits
        };

        let queue_workers = self.workers.entry(key.clone()).or_default();

        if let Some(existing) = queue_workers.get(identity) {
            return Arc::clone(existing.value());
        }

        let permits = Arc::new(TaskPermits::new(
            identity.to_string(),
            task_queue.to_string(),
            namespace.to_string(),
            effective_permits,
        ));

        queue_workers.insert(identity.to_string(), Arc::clone(&permits));

        info!(
            identity = %identity,
            task_queue = %task_queue,
            namespace = %namespace,
            max_permits = effective_permits,
            "Worker registered in capacity registry"
        );

        permits
    }

    pub fn get(
        &self,
        task_queue: &str,
        namespace: &str,
        identity: &str,
    ) -> Option<Arc<TaskPermits>> {
        let key = (task_queue.to_string(), namespace.to_string());
        self.workers
            .get(&key)
            .and_then(|queue_workers| queue_workers.get(identity).map(|p| Arc::clone(p.value())))
    }

    /// Return permits held longer than `ttl` across every worker on this
    /// replica; see [`TaskPermits::release_expired`].
    pub fn release_expired(&self, ttl: Duration) -> usize {
        let mut returned = 0;
        for queue in self.workers.iter() {
            for permits in queue.value().iter() {
                returned += permits.value().release_expired(ttl);
            }
        }
        returned
    }

    pub fn get_or_register(
        &self,
        task_queue: &str,
        namespace: &str,
        identity: &str,
        max_permits: usize,
    ) -> Arc<TaskPermits> {
        self.get(task_queue, namespace, identity)
            .unwrap_or_else(|| self.register(task_queue, namespace, identity, max_permits))
    }

    pub fn unregister(&self, task_queue: &str, namespace: &str, identity: &str) -> bool {
        let key = (task_queue.to_string(), namespace.to_string());
        if let Some(queue_workers) = self.workers.get(&key) {
            let removed = queue_workers.remove(identity).is_some();
            if removed {
                info!(
                    identity = %identity,
                    task_queue = %task_queue,
                    namespace = %namespace,
                    "Worker unregistered from capacity registry"
                );
            }
            return removed;
        }
        false
    }

    pub fn total_available(&self, task_queue: &str, namespace: &str) -> usize {
        let key = (task_queue.to_string(), namespace.to_string());
        self.workers
            .get(&key)
            .map(|queue_workers| queue_workers.iter().map(|w| w.value().available()).sum())
            .unwrap_or(0)
    }

    pub fn total_capacity(&self, task_queue: &str, namespace: &str) -> usize {
        let key = (task_queue.to_string(), namespace.to_string());
        self.workers
            .get(&key)
            .map(|queue_workers| queue_workers.iter().map(|w| w.value().capacity()).sum())
            .unwrap_or(0)
    }

    pub fn total_used(&self, task_queue: &str, namespace: &str) -> usize {
        let key = (task_queue.to_string(), namespace.to_string());
        self.workers
            .get(&key)
            .map(|queue_workers| queue_workers.iter().map(|w| w.value().used()).sum())
            .unwrap_or(0)
    }

    pub fn worker_count(&self, task_queue: &str, namespace: &str) -> usize {
        let key = (task_queue.to_string(), namespace.to_string());
        self.workers
            .get(&key)
            .map(|queue_workers| queue_workers.len())
            .unwrap_or(0)
    }

    pub fn available_worker_count(&self, task_queue: &str, namespace: &str) -> usize {
        let key = (task_queue.to_string(), namespace.to_string());
        self.workers
            .get(&key)
            .map(|queue_workers| {
                queue_workers
                    .iter()
                    .filter(|w| w.value().available() > 0)
                    .count()
            })
            .unwrap_or(0)
    }

    pub fn most_available_worker(
        &self,
        task_queue: &str,
        namespace: &str,
    ) -> Option<Arc<TaskPermits>> {
        let key = (task_queue.to_string(), namespace.to_string());
        self.workers.get(&key).and_then(|queue_workers| {
            queue_workers
                .iter()
                .filter(|w| w.value().available() > 0)
                .max_by_key(|w| w.value().available())
                .map(|w| Arc::clone(w.value()))
        })
    }

    /// Return the permit this replica holds for activation `activation` of
    /// `execution_id`, under whichever worker identity holds it. The activation
    /// number is unique to one hand-out, so this cannot return a permit that
    /// belongs to another; `false` when none is held here.
    pub fn release_activation(
        &self,
        task_queue: &str,
        namespace: &str,
        execution_id: &str,
        activation: i64,
    ) -> bool {
        self.workers(task_queue, namespace)
            .iter()
            .any(|permits| permits.release_for_activation(execution_id, activation))
    }

    pub fn workers(&self, task_queue: &str, namespace: &str) -> Vec<Arc<TaskPermits>> {
        let key = (task_queue.to_string(), namespace.to_string());
        self.workers
            .get(&key)
            .map(|queue_workers| {
                queue_workers
                    .iter()
                    .map(|w| Arc::clone(w.value()))
                    .collect()
            })
            .unwrap_or_default()
    }

    pub fn queue_stats(&self, task_queue: &str, namespace: &str) -> QueueCapacityStats {
        let workers = self.workers(task_queue, namespace);
        let worker_stats: Vec<TaskPermitStats> = workers.iter().map(|w| w.stats()).collect();

        let total_capacity: usize = worker_stats.iter().map(|s| s.max_permits).sum();
        let total_used: usize = worker_stats.iter().map(|s| s.used_permits).sum();
        let total_available: usize = worker_stats.iter().map(|s| s.available_permits).sum();

        QueueCapacityStats {
            task_queue: task_queue.to_string(),
            namespace: namespace.to_string(),
            worker_count: workers.len(),
            total_capacity,
            total_used,
            total_available,
            utilization: if total_capacity > 0 {
                total_used as f64 / total_capacity as f64
            } else {
                0.0
            },
            workers: worker_stats,
        }
    }

    /// Removes workers that have been idle longer than `idle_timeout`.
    pub fn cleanup(&self) -> usize {
        let mut removed = 0;

        self.workers.retain(|key, queue_workers| {
            let before = queue_workers.len();

            queue_workers.retain(|_identity, permits| {
                let last_activity = permits
                    .time_since_last_acquire()
                    .or_else(|| permits.time_since_last_release())
                    .unwrap_or(permits.uptime());

                let keep = last_activity < self.idle_timeout || permits.is_active();

                if !keep {
                    warn!(
                        identity = %permits.identity(),
                        task_queue = %key.0,
                        namespace = %key.1,
                        idle_secs = last_activity.as_secs(),
                        "Removing idle worker from capacity registry"
                    );
                }

                keep
            });

            removed += before - queue_workers.len();
            !queue_workers.is_empty()
        });

        if removed > 0 {
            info!(removed = removed, "Cleaned up idle workers from registry");
        }

        removed
    }

    pub fn global_stats(&self) -> GlobalCapacityStats {
        let mut queue_count = 0;
        let mut total_workers = 0;
        let mut total_capacity = 0;
        let mut total_used = 0;

        for queue in self.workers.iter() {
            queue_count += 1;
            for worker in queue.value().iter() {
                total_workers += 1;
                total_capacity += worker.value().capacity();
                total_used += worker.value().used();
            }
        }

        GlobalCapacityStats {
            queue_count,
            worker_count: total_workers,
            total_capacity,
            total_used,
            total_available: total_capacity.saturating_sub(total_used),
            utilization: if total_capacity > 0 {
                total_used as f64 / total_capacity as f64
            } else {
                0.0
            },
        }
    }
}

impl Default for CapacityRegistry {
    fn default() -> Self {
        Self::new()
    }
}

pub fn create_capacity_registry() -> Arc<CapacityRegistry> {
    Arc::new(CapacityRegistry::new())
}

pub fn create_capacity_registry_with_config(
    default_capacity: usize,
    idle_timeout: Duration,
) -> Arc<CapacityRegistry> {
    Arc::new(CapacityRegistry::with_config(
        default_capacity,
        idle_timeout,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_capacity_registry_register() {
        let registry = CapacityRegistry::new();

        let permits = registry.register("queue", "ns", "worker-1", 100);
        assert_eq!(permits.capacity(), 100);
        assert_eq!(permits.identity(), "worker-1");

        let permits2 = registry.register("queue", "ns", "worker-1", 50);
        assert_eq!(permits2.capacity(), 100);

        let permits3 = registry.register("queue", "ns", "worker-2", 50);
        assert_eq!(permits3.capacity(), 50);

        assert_eq!(registry.worker_count("queue", "ns"), 2);
    }

    #[test]
    fn test_capacity_registry_totals() {
        let registry = CapacityRegistry::new();

        registry.register("queue", "ns", "worker-1", 100);
        registry.register("queue", "ns", "worker-2", 100);

        assert_eq!(registry.total_capacity("queue", "ns"), 200);
        assert_eq!(registry.total_available("queue", "ns"), 200);
        assert_eq!(registry.total_used("queue", "ns"), 0);

        let permits = registry.get("queue", "ns", "worker-1").unwrap();
        permits.try_acquire_many(30);

        assert_eq!(registry.total_capacity("queue", "ns"), 200);
        assert_eq!(registry.total_available("queue", "ns"), 170);
        assert_eq!(registry.total_used("queue", "ns"), 30);
    }

    #[test]
    fn test_capacity_registry_most_available() {
        let registry = CapacityRegistry::new();

        let w1 = registry.register("queue", "ns", "worker-1", 100);
        let _w2 = registry.register("queue", "ns", "worker-2", 100);

        let most = registry.most_available_worker("queue", "ns");
        assert!(most.is_some());

        w1.try_acquire_many(80);

        let most = registry.most_available_worker("queue", "ns").unwrap();
        assert_eq!(most.identity(), "worker-2");
    }

    #[test]
    fn test_capacity_registry_unregister() {
        let registry = CapacityRegistry::new();

        registry.register("queue", "ns", "worker-1", 100);
        assert_eq!(registry.worker_count("queue", "ns"), 1);

        registry.unregister("queue", "ns", "worker-1");
        assert_eq!(registry.worker_count("queue", "ns"), 0);
    }

    #[test]
    fn test_queue_stats() {
        let registry = CapacityRegistry::new();

        let w1 = registry.register("queue", "ns", "worker-1", 100);
        let _w2 = registry.register("queue", "ns", "worker-2", 50);

        w1.try_acquire_many(30);

        let stats = registry.queue_stats("queue", "ns");
        assert_eq!(stats.worker_count, 2);
        assert_eq!(stats.total_capacity, 150);
        assert_eq!(stats.total_used, 30);
        assert_eq!(stats.total_available, 120);
        assert!((stats.utilization - 0.2).abs() < 0.001);
    }

    #[test]
    fn test_global_stats() {
        let registry = CapacityRegistry::new();

        registry.register("queue1", "ns", "worker-1", 100);
        registry.register("queue2", "ns", "worker-2", 50);

        let stats = registry.global_stats();
        assert_eq!(stats.queue_count, 2);
        assert_eq!(stats.worker_count, 2);
        assert_eq!(stats.total_capacity, 150);
    }
}
