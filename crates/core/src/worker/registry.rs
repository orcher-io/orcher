//! In-memory worker registry.

use dashmap::DashMap;
use jiff::SignedDuration;
use orcher_common::types::time::Timestamptz;
use orcher_common::OrcherResult;
use orcher_proto::orcher::v1::{WorkerLoadMetrics, WorkerRegistrationStatus};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, info, warn};
use uuid::Uuid;

/// Worker capabilities (what workflow/task types it can execute)
#[derive(Debug, Clone)]
pub struct WorkerCapabilities {
    /// Workflow types this worker can execute
    pub workflow_types: Vec<String>,

    /// Task types this worker can execute
    pub task_types: Vec<String>,

    /// Maximum concurrent workflow executions
    pub max_concurrent_workflows: u32,

    /// Maximum concurrent task executions
    pub max_concurrent_tasks: u32,
}

/// Information about a registered worker
#[derive(Debug, Clone)]
pub struct WorkerInfo {
    /// Unique service instance identifier
    pub service_id: String,

    /// Human-readable worker identity
    pub identity: String,

    /// Task queue this worker polls
    pub task_queue: String,

    /// Namespace this worker operates in
    pub namespace: String,

    /// Worker capabilities
    pub capabilities: WorkerCapabilities,

    /// Current status (from proto enum)
    pub status: i32,

    /// Registration timestamp
    pub registered_at: Timestamptz,

    /// Last heartbeat timestamp
    pub last_heartbeat: Timestamptz,

    /// Additional metadata (sdk version, build id, host info)
    pub metadata: HashMap<String, String>,

    /// Latest load metrics
    pub metrics: Option<WorkerLoadMetrics>,

    /// Organisation the registering caller authenticated as.
    ///
    /// `None` when the server runs without tenancy. Persisted on the worker
    /// row so an organisation's worker list shows its own workers; without it
    /// every worker was stored under the nil organisation and no tenant saw
    /// any of them.
    pub organization_id: Option<uuid::Uuid>,
}

impl WorkerInfo {
    /// Check if worker is healthy (recent heartbeat + healthy status)
    pub fn is_healthy(&self, timeout: SignedDuration) -> bool {
        let elapsed = Timestamptz::now() - self.last_heartbeat;
        elapsed < timeout && self.status == (WorkerRegistrationStatus::Healthy as i32)
    }
}

/// In-memory registry for tracking connected workers
#[derive(Debug, Clone)]
pub struct WorkerRegistry {
    /// Map of service_id → WorkerInfo
    workers: Arc<DashMap<String, WorkerInfo>>,

    /// Map of registration_id → service_id
    registrations: Arc<DashMap<String, String>>,

    /// Heartbeat timeout duration (default: 30 seconds)
    heartbeat_timeout: SignedDuration,
}

impl WorkerRegistry {
    /// Create a new worker registry with default 30s heartbeat timeout
    pub fn new() -> Self {
        Self {
            workers: Arc::new(DashMap::new()),
            registrations: Arc::new(DashMap::new()),
            heartbeat_timeout: SignedDuration::from_secs(30),
        }
    }

    /// Register a worker and return a unique registration ID
    pub async fn register(&self, worker_info: WorkerInfo) -> OrcherResult<String> {
        let service_id = worker_info.service_id.clone();
        let registration_id = Uuid::new_v4().to_string();

        info!(
            service_id = %service_id,
            identity = %worker_info.identity,
            task_queue = %worker_info.task_queue,
            namespace = %worker_info.namespace,
            workflow_types = ?worker_info.capabilities.workflow_types,
            task_types = ?worker_info.capabilities.task_types,
            "Worker registered"
        );

        // Store worker info (overwrites previous registration for same service_id)
        self.workers.insert(service_id.clone(), worker_info);

        // Store registration mapping
        self.registrations
            .insert(registration_id.clone(), service_id);

        Ok(registration_id)
    }

    /// Deregister a worker
    pub async fn deregister(&self, service_id: &str) -> OrcherResult<bool> {
        let removed = self.workers.remove(service_id).is_some();
        if removed {
            self.registrations.retain(|_, v| v != service_id);
            info!(service_id = %service_id, "Worker deregistered");
        } else {
            warn!(service_id = %service_id, "Worker not found for deregistration");
        }
        Ok(removed)
    }

    /// Update worker heartbeat, returns false if worker not found (needs re-registration)
    pub async fn heartbeat(
        &self,
        service_id: &str,
        status: i32,
        metrics: Option<WorkerLoadMetrics>,
    ) -> OrcherResult<bool> {
        let mut worker = match self.workers.get_mut(service_id) {
            Some(w) => w,
            None => {
                debug!(service_id = %service_id, "Heartbeat from unknown worker");
                return Ok(false);
            }
        };

        worker.last_heartbeat = Timestamptz::now();
        worker.status = status;
        if let Some(m) = metrics {
            worker.metrics = Some(m);
        }

        Ok(true)
    }

    /// Get worker info by service_id
    pub async fn get_worker(&self, service_id: &str) -> Option<WorkerInfo> {
        self.workers.get(service_id).map(|e| e.value().clone())
    }

    /// Get all registered workers
    pub async fn list_workers(&self) -> Vec<WorkerInfo> {
        self.workers.iter().map(|e| e.value().clone()).collect()
    }

    /// Get workers by task queue
    pub async fn get_workers_by_queue(&self, task_queue: &str) -> Vec<WorkerInfo> {
        self.workers
            .iter()
            .filter(|e| e.value().task_queue == task_queue)
            .map(|e| e.value().clone())
            .collect()
    }

    /// Get healthy workers
    pub async fn list_healthy_workers(&self) -> Vec<WorkerInfo> {
        self.workers
            .iter()
            .filter(|e| e.value().is_healthy(self.heartbeat_timeout))
            .map(|e| e.value().clone())
            .collect()
    }

    /// Count registered workers
    pub fn worker_count(&self) -> usize {
        self.workers.len()
    }

    /// Count healthy workers
    pub async fn healthy_worker_count(&self) -> usize {
        self.workers
            .iter()
            .filter(|e| e.value().is_healthy(self.heartbeat_timeout))
            .count()
    }

    /// Clean up stale workers (no heartbeat within 2x timeout)
    pub async fn cleanup_stale_workers(&self) -> usize {
        let now = Timestamptz::now();
        let stale: Vec<String> = self
            .workers
            .iter()
            .filter(|e| {
                let elapsed = now - e.value().last_heartbeat;
                elapsed > self.heartbeat_timeout * 2
            })
            .map(|e| e.key().clone())
            .collect();

        let count = stale.len();
        for service_id in stale {
            info!(service_id = %service_id, "Removing stale worker");
            self.workers.remove(&service_id);
            self.registrations.retain(|_, v| v.as_str() != service_id);
        }

        if count > 0 {
            info!(count = count, "Cleaned up stale workers");
        }

        count
    }
}

impl Default for WorkerRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_worker(service_id: &str) -> WorkerInfo {
        WorkerInfo {
            service_id: service_id.to_string(),
            identity: format!("{}-identity", service_id),
            task_queue: "default-queue".to_string(),
            namespace: "default".to_string(),
            capabilities: WorkerCapabilities {
                workflow_types: vec!["OrderWorkflow".to_string()],
                task_types: vec!["SendEmail".to_string()],
                max_concurrent_workflows: 100,
                max_concurrent_tasks: 200,
            },
            status: WorkerRegistrationStatus::Healthy as i32,
            registered_at: Timestamptz::now(),
            last_heartbeat: Timestamptz::now(),
            metadata: HashMap::new(),
            metrics: None,
            organization_id: None,
        }
    }

    #[tokio::test]
    async fn test_register_worker() {
        let registry = WorkerRegistry::new();
        let reg_id = registry.register(test_worker("w1")).await.unwrap();
        assert!(!reg_id.is_empty());
        assert_eq!(registry.worker_count(), 1);
    }

    #[tokio::test]
    async fn test_deregister_worker() {
        let registry = WorkerRegistry::new();
        registry.register(test_worker("w1")).await.unwrap();
        assert!(registry.deregister("w1").await.unwrap());
        assert_eq!(registry.worker_count(), 0);
    }

    #[tokio::test]
    async fn test_heartbeat() {
        let registry = WorkerRegistry::new();
        registry.register(test_worker("w1")).await.unwrap();
        assert!(registry
            .heartbeat("w1", WorkerRegistrationStatus::Healthy as i32, None)
            .await
            .unwrap());
        assert!(!registry
            .heartbeat("unknown", WorkerRegistrationStatus::Healthy as i32, None)
            .await
            .unwrap());
    }

    #[tokio::test]
    async fn test_cleanup_stale() {
        let registry = WorkerRegistry::new();
        let mut w = test_worker("w1");
        w.last_heartbeat = Timestamptz::now() - SignedDuration::from_secs(120);
        registry.register(w).await.unwrap();
        assert_eq!(registry.cleanup_stale_workers().await, 1);
        assert_eq!(registry.worker_count(), 0);
    }

    #[tokio::test]
    async fn test_get_workers_by_queue() {
        let registry = WorkerRegistry::new();
        registry.register(test_worker("w1")).await.unwrap();
        let mut w2 = test_worker("w2");
        w2.task_queue = "other-queue".to_string();
        registry.register(w2).await.unwrap();

        let workers = registry.get_workers_by_queue("default-queue").await;
        assert_eq!(workers.len(), 1);
        assert_eq!(workers[0].service_id, "w1");
    }
}
