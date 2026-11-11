//! Worker affinity leases - routes consecutive workflow tasks to the same worker for cache locality.

use dashmap::DashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tracing::{debug, trace};
use uuid::Uuid;

#[derive(Clone, Debug)]
pub struct WorkerAffinityConfig {
    pub enabled: bool,
    pub default_lease: Duration,
    /// Cap to prevent permanent affinity causing load imbalance.
    pub max_lease: Duration,
    pub cleanup_interval: Duration,
}

impl Default for WorkerAffinityConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            default_lease: Duration::from_secs(30),
            max_lease: Duration::from_secs(300), // 5 minutes max
            cleanup_interval: Duration::from_secs(30),
        }
    }
}

impl WorkerAffinityConfig {
    pub fn disabled() -> Self {
        Self {
            enabled: false,
            ..Default::default()
        }
    }

    pub fn with_lease_durations(default_lease: Duration, max_lease: Duration) -> Self {
        Self {
            enabled: true,
            default_lease,
            max_lease,
            cleanup_interval: Duration::from_secs(30),
        }
    }
}

/// Time-limited preference for a workflow-to-worker mapping.
#[derive(Debug, Clone)]
pub struct AffinityLease {
    pub worker_id: String,
    pub expires_at: Instant,
    pub task_count: u64,
    pub created_at: Instant,
}

impl AffinityLease {
    pub fn new(worker_id: String, duration: Duration) -> Self {
        let now = Instant::now();
        Self {
            worker_id,
            expires_at: now + duration,
            task_count: 1,
            created_at: now,
        }
    }

    #[inline]
    pub fn is_expired(&self) -> bool {
        Instant::now() >= self.expires_at
    }

    pub fn remaining_time(&self) -> Duration {
        self.expires_at.saturating_duration_since(Instant::now())
    }

    pub fn age(&self) -> Duration {
        self.created_at.elapsed()
    }
}

#[derive(Debug, Default)]
pub struct AffinityMetrics {
    pub hits: AtomicU64,
    pub misses: AtomicU64,
    pub leases_created: AtomicU64,
    pub renewals: AtomicU64,
    pub expirations: AtomicU64,
    pub clears: AtomicU64,
}

impl AffinityMetrics {
    pub fn new() -> Self {
        Self::default()
    }

    #[inline]
    pub fn record_hit(&self) {
        self.hits.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn record_miss(&self) {
        self.misses.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn record_lease_created(&self) {
        self.leases_created.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn record_renewal(&self) {
        self.renewals.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn record_expiration(&self) {
        self.expirations.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn record_clear(&self) {
        self.clears.fetch_add(1, Ordering::Relaxed);
    }

    pub fn hit_rate(&self) -> f64 {
        let hits = self.hits.load(Ordering::Relaxed);
        let misses = self.misses.load(Ordering::Relaxed);
        let total = hits + misses;
        if total == 0 {
            0.0
        } else {
            hits as f64 / total as f64
        }
    }
}

/// Thread-safe affinity manager backed by `DashMap` (~150 bytes per lease).
pub struct WorkerAffinityManager {
    leases: DashMap<Uuid, AffinityLease>,
    config: WorkerAffinityConfig,
    metrics: AffinityMetrics,
}

impl WorkerAffinityManager {
    pub fn new(config: WorkerAffinityConfig) -> Self {
        debug!(
            enabled = config.enabled,
            default_lease_secs = config.default_lease.as_secs(),
            max_lease_secs = config.max_lease.as_secs(),
            "Creating WorkerAffinityManager"
        );

        Self {
            leases: DashMap::new(),
            config,
            metrics: AffinityMetrics::new(),
        }
    }

    pub fn with_defaults() -> Self {
        Self::new(WorkerAffinityConfig::default())
    }

    #[inline]
    pub fn is_enabled(&self) -> bool {
        self.config.enabled
    }

    /// Returns the worker with a valid lease for this workflow, or `None`.
    pub fn get_preferred_worker(&self, workflow_id: &Uuid) -> Option<String> {
        if !self.config.enabled {
            return None;
        }

        match self.leases.get(workflow_id) {
            Some(lease) => {
                if lease.is_expired() {
                    self.metrics.record_miss();
                    self.metrics.record_expiration();
                    trace!(
                        workflow_id = %workflow_id,
                        worker_id = %lease.worker_id,
                        "Affinity lease expired"
                    );
                    None
                } else {
                    self.metrics.record_hit();
                    trace!(
                        workflow_id = %workflow_id,
                        worker_id = %lease.worker_id,
                        remaining_secs = lease.remaining_time().as_secs(),
                        task_count = lease.task_count,
                        "Affinity hit"
                    );
                    Some(lease.worker_id.clone())
                }
            }
            None => {
                self.metrics.record_miss();
                None
            }
        }
    }

    /// Call after a worker successfully processes a task for a workflow.
    pub fn set_preferred_worker(
        &self,
        workflow_id: Uuid,
        worker_id: String,
        lease_duration: Option<Duration>,
    ) {
        if !self.config.enabled {
            return;
        }

        let duration = lease_duration
            .unwrap_or(self.config.default_lease)
            .min(self.config.max_lease);

        let lease = AffinityLease::new(worker_id.clone(), duration);

        let is_new = self
            .leases
            .insert(workflow_id, lease.clone())
            .map(|old| old.worker_id != worker_id)
            .unwrap_or(true);

        if is_new {
            self.metrics.record_lease_created();
            debug!(
                workflow_id = %workflow_id,
                worker_id = %worker_id,
                lease_secs = duration.as_secs(),
                "Created affinity lease"
            );
        } else {
            self.metrics.record_renewal();
            trace!(
                workflow_id = %workflow_id,
                worker_id = %worker_id,
                "Updated affinity lease"
            );
        }
    }

    /// Only renews if the current lease belongs to this worker.
    pub fn renew_lease(&self, workflow_id: &Uuid, worker_id: &str) {
        if !self.config.enabled {
            return;
        }

        if let Some(mut lease) = self.leases.get_mut(workflow_id) {
            if lease.worker_id == worker_id {
                let new_expires_at = Instant::now() + self.config.default_lease;
                let max_expires_at = lease.created_at + self.config.max_lease;
                lease.expires_at = new_expires_at.min(max_expires_at);
                lease.task_count += 1;

                self.metrics.record_renewal();
                trace!(
                    workflow_id = %workflow_id,
                    worker_id = %worker_id,
                    task_count = lease.task_count,
                    remaining_secs = lease.remaining_time().as_secs(),
                    "Renewed affinity lease"
                );
            }
        }
    }

    pub fn clear_lease(&self, workflow_id: &Uuid) {
        if let Some((_, lease)) = self.leases.remove(workflow_id) {
            self.metrics.record_clear();
            debug!(
                workflow_id = %workflow_id,
                worker_id = %lease.worker_id,
                task_count = lease.task_count,
                age_secs = lease.age().as_secs(),
                "Cleared affinity lease on workflow completion"
            );
        }
    }

    /// Returns the number of expired leases removed.
    pub fn cleanup_expired(&self) -> usize {
        let mut expired_count = 0;

        self.leases.retain(|workflow_id, lease| {
            if lease.is_expired() {
                expired_count += 1;
                self.metrics.record_expiration();
                trace!(
                    workflow_id = %workflow_id,
                    worker_id = %lease.worker_id,
                    task_count = lease.task_count,
                    "Removing expired affinity lease"
                );
                false
            } else {
                true
            }
        });

        if expired_count > 0 {
            debug!(
                expired_count,
                remaining_leases = self.leases.len(),
                "Cleaned up expired affinity leases"
            );
        }

        expired_count
    }

    pub fn lease_count(&self) -> usize {
        self.leases.len()
    }

    pub fn metrics(&self) -> &AffinityMetrics {
        &self.metrics
    }

    pub fn config(&self) -> &WorkerAffinityConfig {
        &self.config
    }

    pub fn get_lease(&self, workflow_id: &Uuid) -> Option<AffinityLease> {
        self.leases.get(workflow_id).map(|l| l.clone())
    }
}

impl std::fmt::Debug for WorkerAffinityManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkerAffinityManager")
            .field("enabled", &self.config.enabled)
            .field("lease_count", &self.leases.len())
            .field("default_lease_secs", &self.config.default_lease.as_secs())
            .field("max_lease_secs", &self.config.max_lease.as_secs())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_set_get_preferred_worker() {
        let manager = WorkerAffinityManager::with_defaults();
        let workflow_id = Uuid::new_v4();

        assert!(manager.get_preferred_worker(&workflow_id).is_none());

        manager.set_preferred_worker(workflow_id, "worker-1".to_string(), None);

        assert_eq!(
            manager.get_preferred_worker(&workflow_id),
            Some("worker-1".to_string())
        );
    }

    #[test]
    fn test_lease_expiration() {
        let config = WorkerAffinityConfig {
            enabled: true,
            default_lease: Duration::from_millis(1),
            max_lease: Duration::from_millis(10),
            cleanup_interval: Duration::from_secs(30),
        };
        let manager = WorkerAffinityManager::new(config);
        let workflow_id = Uuid::new_v4();

        manager.set_preferred_worker(workflow_id, "worker-1".to_string(), None);

        std::thread::sleep(Duration::from_millis(5));

        assert!(manager.get_preferred_worker(&workflow_id).is_none());
        assert_eq!(manager.metrics().expirations.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn test_lease_renewal() {
        let manager = WorkerAffinityManager::with_defaults();
        let workflow_id = Uuid::new_v4();

        manager.set_preferred_worker(workflow_id, "worker-1".to_string(), None);

        let initial_lease = manager.get_lease(&workflow_id).unwrap();
        assert_eq!(initial_lease.task_count, 1);

        manager.renew_lease(&workflow_id, "worker-1");

        let renewed_lease = manager.get_lease(&workflow_id).unwrap();
        assert_eq!(renewed_lease.task_count, 2);
    }

    #[test]
    fn test_renewal_only_for_same_worker() {
        let manager = WorkerAffinityManager::with_defaults();
        let workflow_id = Uuid::new_v4();

        manager.set_preferred_worker(workflow_id, "worker-1".to_string(), None);
        manager.renew_lease(&workflow_id, "worker-2");

        let lease = manager.get_lease(&workflow_id).unwrap();
        assert_eq!(lease.task_count, 1);
    }

    #[test]
    fn test_clear_lease() {
        let manager = WorkerAffinityManager::with_defaults();
        let workflow_id = Uuid::new_v4();

        manager.set_preferred_worker(workflow_id, "worker-1".to_string(), None);
        assert!(manager.get_preferred_worker(&workflow_id).is_some());

        manager.clear_lease(&workflow_id);

        assert!(manager.get_preferred_worker(&workflow_id).is_none());
        assert_eq!(manager.metrics().clears.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn test_cleanup_expired() {
        let config = WorkerAffinityConfig {
            enabled: true,
            default_lease: Duration::from_millis(1),
            max_lease: Duration::from_millis(10),
            cleanup_interval: Duration::from_secs(30),
        };
        let manager = WorkerAffinityManager::new(config);

        for i in 0..5 {
            manager.set_preferred_worker(Uuid::new_v4(), format!("worker-{}", i), None);
        }
        assert_eq!(manager.lease_count(), 5);

        std::thread::sleep(Duration::from_millis(5));

        let expired = manager.cleanup_expired();
        assert_eq!(expired, 5);
        assert_eq!(manager.lease_count(), 0);
    }

    #[test]
    fn test_disabled_affinity() {
        let manager = WorkerAffinityManager::new(WorkerAffinityConfig::disabled());
        let workflow_id = Uuid::new_v4();

        manager.set_preferred_worker(workflow_id, "worker-1".to_string(), None);

        assert!(manager.get_preferred_worker(&workflow_id).is_none());
        assert_eq!(manager.lease_count(), 0);
    }

    #[test]
    fn test_max_lease_cap() {
        let config = WorkerAffinityConfig {
            enabled: true,
            default_lease: Duration::from_secs(60),
            max_lease: Duration::from_secs(30),
            cleanup_interval: Duration::from_secs(30),
        };
        let manager = WorkerAffinityManager::new(config);
        let workflow_id = Uuid::new_v4();

        manager.set_preferred_worker(
            workflow_id,
            "worker-1".to_string(),
            Some(Duration::from_secs(1000)),
        );

        let lease = manager.get_lease(&workflow_id).unwrap();
        assert!(lease.remaining_time() <= Duration::from_secs(30));
    }

    #[test]
    fn test_update_affinity_to_different_worker() {
        let manager = WorkerAffinityManager::with_defaults();
        let workflow_id = Uuid::new_v4();

        manager.set_preferred_worker(workflow_id, "worker-1".to_string(), None);
        assert_eq!(
            manager.get_preferred_worker(&workflow_id),
            Some("worker-1".to_string())
        );

        manager.set_preferred_worker(workflow_id, "worker-2".to_string(), None);
        assert_eq!(
            manager.get_preferred_worker(&workflow_id),
            Some("worker-2".to_string())
        );
    }

    #[test]
    fn test_metrics_hit_rate() {
        let manager = WorkerAffinityManager::with_defaults();
        let workflow_id = Uuid::new_v4();

        manager.get_preferred_worker(&workflow_id);

        manager.set_preferred_worker(workflow_id, "worker-1".to_string(), None);

        manager.get_preferred_worker(&workflow_id);
        manager.get_preferred_worker(&workflow_id);

        let rate = manager.metrics().hit_rate();
        assert!((rate - 0.6666).abs() < 0.01);
    }

    #[test]
    fn test_affinity_lease_methods() {
        let lease = AffinityLease::new("worker-1".to_string(), Duration::from_secs(60));

        assert!(!lease.is_expired());
        assert!(lease.remaining_time() > Duration::from_secs(50));
        assert!(lease.age() < Duration::from_secs(1));
        assert_eq!(lease.task_count, 1);
    }
}
