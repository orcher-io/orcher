//! Service registration and handler tracking for actors.

use dashmap::DashMap;
use jiff::SignedDuration;
use orcher_common::types::time::Timestamptz;
use orcher_common::OrcherResult;
use orcher_proto::orcher::v1::{OperationMode, WorkerMetrics, WorkerStatus};
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tracing::{debug, info, warn};
use uuid::Uuid;

/// Handler information
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HandlerInfo {
    /// Actor type name (e.g., "ShoppingCart")
    pub actor_name: String,

    /// Operation name (e.g., "add_item")
    pub operation: String,

    /// Operation mode (exclusive or shared)
    pub mode: OperationMode,

    /// Optional handler metadata
    pub metadata: HashMap<String, String>,
}

/// Service information
///
/// Represents a deployed service (execution runtime) that can execute
/// actor handlers. Services are applications written by developers using Orcher SDKs.
#[derive(Debug, Clone)]
pub struct ServiceInfo {
    /// Unique service identifier (e.g., "order-service-1", "cart-service-prod-2")
    pub service_id: String,

    /// Service endpoint for RPC calls
    pub endpoint: String,

    /// List of handlers this service provides
    pub handlers: Vec<HandlerInfo>,

    /// Current service status (from proto)
    pub status: i32,

    /// Registration timestamp
    pub registered_at: Timestamptz,

    /// Last heartbeat timestamp
    pub last_heartbeat: Timestamptz,

    /// Service metadata (version, environment, region, etc.)
    pub metadata: HashMap<String, String>,

    /// Latest service metrics
    pub metrics: Option<WorkerMetrics>,
}

impl ServiceInfo {
    /// Check if service is healthy (recent heartbeat)
    pub fn is_healthy(&self, timeout: SignedDuration) -> bool {
        let now = Timestamptz::now();
        let elapsed = now - self.last_heartbeat;
        elapsed < timeout && self.status == (WorkerStatus::Healthy as i32)
    }

    /// Check if service can handle a specific operation
    pub fn can_handle(&self, actor_name: &str, operation: &str) -> bool {
        self.handlers
            .iter()
            .any(|h| h.actor_name == actor_name && h.operation == operation)
    }

    /// Compute a load score for service selection (lower = less loaded = preferred).
    ///
    /// Primary signal: `operations_in_progress` (×1000).
    /// Secondary signal: `cpu_usage` (×10, to keep it sub-primary).
    /// No metrics reported → `u64::MAX` (deprioritised, used as round-robin fallback).
    pub fn load_score(&self) -> u64 {
        match &self.metrics {
            Some(m) => {
                let ops = m.operations_in_progress as u64;
                let cpu = (m.cpu_usage * 10.0) as u64;
                ops * 1_000 + cpu
            }
            None => u64::MAX,
        }
    }

    /// Get operation mode for a handler
    pub fn get_operation_mode(&self, actor_name: &str, operation: &str) -> Option<OperationMode> {
        self.handlers
            .iter()
            .find(|h| h.actor_name == actor_name && h.operation == operation)
            .map(|h| h.mode)
    }
}

/// Service registry for tracking active services and their handlers
///
/// In Orcher terminology:
/// - "Service" = Execution runtime that runs handlers
/// - "Handler" = Actor operation implementation
/// - "Registration" = Service announcing its capabilities
#[derive(Debug, Clone)]
pub struct ServiceRegistry {
    /// Map of service_id → ServiceInfo
    services: Arc<DashMap<String, ServiceInfo>>,

    /// Map of (actor_name, operation) → Vec<service_id>
    handlers: Arc<DashMap<(String, String), Vec<String>>>,

    /// Map of registration_id → service_id
    registrations: Arc<DashMap<String, String>>,

    /// Heartbeat timeout duration (default: 30 seconds)
    heartbeat_timeout: SignedDuration,

    /// Monotonically increasing counter used for round-robin tiebreaking.
    /// Shared across all clones so rotation is global.
    round_robin_counter: Arc<AtomicUsize>,
}

impl ServiceRegistry {
    /// Create a new service registry
    pub fn new() -> Self {
        Self {
            services: Arc::new(DashMap::new()),
            handlers: Arc::new(DashMap::new()),
            registrations: Arc::new(DashMap::new()),
            heartbeat_timeout: SignedDuration::from_secs(30),
            round_robin_counter: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Create a new service registry with custom heartbeat timeout
    pub fn new_with_timeout(heartbeat_timeout: SignedDuration) -> Self {
        Self {
            services: Arc::new(DashMap::new()),
            handlers: Arc::new(DashMap::new()),
            registrations: Arc::new(DashMap::new()),
            heartbeat_timeout,
            round_robin_counter: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Register a service and its handlers
    ///
    /// Returns a unique registration ID
    pub async fn register(&self, service_info: ServiceInfo) -> OrcherResult<String> {
        let service_id = service_info.service_id.clone();
        let registration_id = Uuid::new_v4().to_string();

        info!(
            "Registering service: id={}, endpoint={}, handlers={}",
            service_id,
            service_info.endpoint,
            service_info.handlers.len()
        );

        // Register each handler
        for handler in &service_info.handlers {
            let key = (handler.actor_name.clone(), handler.operation.clone());
            self.handlers
                .entry(key.clone())
                .or_insert_with(Vec::new)
                .push(service_id.clone());

            debug!(
                "Registered handler: actor={}, operation={}, service={}",
                handler.actor_name, handler.operation, service_id
            );
        }

        // Store service info
        self.services.insert(service_id.clone(), service_info);

        // Store registration mapping
        self.registrations
            .insert(registration_id.clone(), service_id.clone());

        info!(
            "Service registered successfully: id={}, registration_id={}",
            service_id, registration_id
        );

        Ok(registration_id)
    }

    /// Unregister a service
    pub async fn unregister(&self, service_id: &str) -> OrcherResult<bool> {
        info!("Unregistering service: id={}", service_id);

        // Remove service info
        let service_info = match self.services.remove(service_id) {
            Some((_, info)) => info,
            None => {
                warn!("Service not found for unregistration: id={}", service_id);
                return Ok(false);
            }
        };

        // Remove handler mappings
        for handler in &service_info.handlers {
            let key = (handler.actor_name.clone(), handler.operation.clone());
            if let Some(mut services) = self.handlers.get_mut(&key) {
                services.retain(|s| s != service_id);
                if services.is_empty() {
                    drop(services);
                    self.handlers.remove(&key);
                }
            }
        }

        // Remove registration mappings
        self.registrations.retain(|_, v| v != service_id);

        info!("Service unregistered successfully: id={}", service_id);

        Ok(true)
    }

    /// Get service information
    pub async fn get_service(&self, service_id: &str) -> Option<ServiceInfo> {
        self.services
            .get(service_id)
            .map(|entry| entry.value().clone())
    }

    /// Get service by registration ID
    pub async fn get_service_by_registration(&self, registration_id: &str) -> Option<ServiceInfo> {
        let service_id = self.registrations.get(registration_id)?.value().clone();
        self.get_service(&service_id).await
    }

    /// Find services that can handle a specific operation
    pub async fn find_services(&self, actor_name: &str, operation: &str) -> Vec<ServiceInfo> {
        let key = (actor_name.to_string(), operation.to_string());

        let service_ids = match self.handlers.get(&key) {
            Some(ids) => ids.value().clone(),
            None => {
                debug!(
                    "No services found for handler: actor={}, operation={}",
                    actor_name, operation
                );
                return Vec::new();
            }
        };

        let mut services = Vec::new();
        for service_id in service_ids {
            if let Some(service_info) = self.services.get(&service_id) {
                if service_info.is_healthy(self.heartbeat_timeout) {
                    services.push(service_info.value().clone());
                } else {
                    debug!("Skipping unhealthy service: id={}", service_id);
                }
            }
        }

        debug!(
            "Found {} healthy services for actor={}, operation={}",
            services.len(),
            actor_name,
            operation
        );

        services
    }

    /// Select the best healthy service for an operation.
    ///
    /// Strategy:
    /// 1. Sort candidates by `load_score` (operations_in_progress × 1000 + cpu × 10).
    /// 2. Among services tied on the lowest score, apply round-robin rotation so
    ///    traffic spreads evenly across equally-loaded instances.
    /// 3. Services that report no metrics are sorted last and also round-robined.
    pub async fn select_service(&self, actor_name: &str, operation: &str) -> Option<ServiceInfo> {
        let mut services = self.find_services(actor_name, operation).await;

        if services.is_empty() {
            return None;
        }

        if services.len() == 1 {
            return services.into_iter().next();
        }

        // Sort by load score ascending so the least-loaded candidate comes first
        services.sort_by_key(|s| s.load_score());

        let best_score = services[0].load_score();
        let tied: Vec<ServiceInfo> = services
            .into_iter()
            .take_while(|s| s.load_score() == best_score)
            .collect();

        // Round-robin among tied candidates
        let idx = self.round_robin_counter.fetch_add(1, Ordering::Relaxed) % tied.len();

        debug!(
            actor = %actor_name,
            operation = %operation,
            candidates = tied.len(),
            selected_idx = idx,
            load_score = best_score,
            "Selected service via least-loaded + round-robin"
        );

        tied.into_iter().nth(idx)
    }

    /// Update service heartbeat
    pub async fn heartbeat(
        &self,
        service_id: &str,
        status: i32,
        metrics: Option<WorkerMetrics>,
    ) -> OrcherResult<bool> {
        let mut service = match self.services.get_mut(service_id) {
            Some(s) => s,
            None => {
                warn!(
                    "Heartbeat received from unregistered service: id={}",
                    service_id
                );
                return Ok(false);
            }
        };

        service.last_heartbeat = Timestamptz::now();
        service.status = status;
        if let Some(m) = metrics {
            service.metrics = Some(m);
        }

        debug!("Heartbeat updated for service: id={}", service_id);

        Ok(true)
    }

    /// Get all registered services
    pub async fn list_services(&self) -> Vec<ServiceInfo> {
        self.services
            .iter()
            .map(|entry| entry.value().clone())
            .collect()
    }

    /// Get all healthy services
    pub async fn list_healthy_services(&self) -> Vec<ServiceInfo> {
        self.services
            .iter()
            .filter(|entry| entry.value().is_healthy(self.heartbeat_timeout))
            .map(|entry| entry.value().clone())
            .collect()
    }

    /// Get count of registered services
    pub async fn service_count(&self) -> usize {
        self.services.len()
    }

    /// Get count of healthy services
    pub async fn healthy_service_count(&self) -> usize {
        self.services
            .iter()
            .filter(|entry| entry.value().is_healthy(self.heartbeat_timeout))
            .count()
    }

    /// Clean up stale services (no heartbeat within timeout)
    pub async fn cleanup_stale_services(&self) -> usize {
        let now = Timestamptz::now();
        let mut removed = 0;

        let stale_services: Vec<String> = self
            .services
            .iter()
            .filter(|entry| {
                let elapsed = now - entry.value().last_heartbeat;
                elapsed > self.heartbeat_timeout * 2 // 2x timeout = stale
            })
            .map(|entry| entry.key().clone())
            .collect();

        for service_id in stale_services {
            info!("Removing stale service: id={}", service_id);
            if self.unregister(&service_id).await.unwrap_or(false) {
                removed += 1;
            }
        }

        if removed > 0 {
            info!("Cleaned up {} stale services", removed);
        }

        removed
    }

    /// Get handler statistics
    pub async fn handler_stats(&self) -> HashMap<(String, String), usize> {
        self.handlers
            .iter()
            .map(|entry| (entry.key().clone(), entry.value().len()))
            .collect()
    }

    /// Check if a handler is registered
    pub async fn has_handler(&self, actor_name: &str, operation: &str) -> bool {
        let key = (actor_name.to_string(), operation.to_string());
        self.handlers.contains_key(&key)
    }

    /// Resolve the effective operation mode for `(actor_name, operation)`
    /// across all registered workers.
    ///
    /// The handler's declared mode (from `#[operation(shared)]`,
    /// `@operation(mode=SHARED)`, `@Operation({mode: 'shared'})`) is the
    /// authoritative source — callers cannot request a mode.
    ///
    /// Resolution is deliberately conservative for mixed fleets (e.g. a
    /// rolling deploy where old and new workers declare different modes for
    /// the same operation): if ANY registered handler declares the operation
    /// exclusive — or the operation is unknown — it runs exclusive. Only a
    /// unanimous `Shared` declaration yields `Shared`.
    pub async fn resolve_operation_mode(&self, actor_name: &str, operation: &str) -> OperationMode {
        let key = (actor_name.to_string(), operation.to_string());

        let service_ids = match self.handlers.get(&key) {
            Some(ids) => ids.value().clone(),
            None => return OperationMode::Exclusive,
        };

        let mut saw_shared = false;
        for service_id in service_ids {
            if let Some(service) = self.services.get(&service_id) {
                // Only the CURRENT fleet votes: dead workers' registrations
                // linger until reaped, and a stale exclusive declaration must
                // not pin an operation exclusive forever.
                if !service.is_healthy(self.heartbeat_timeout) {
                    continue;
                }
                match service.get_operation_mode(actor_name, operation) {
                    Some(OperationMode::Shared) => saw_shared = true,
                    // Any exclusive (or unspecified) declaration wins:
                    // concurrent execution must be unanimously opted into.
                    Some(_) => return OperationMode::Exclusive,
                    None => {}
                }
            }
        }

        if saw_shared {
            OperationMode::Shared
        } else {
            OperationMode::Exclusive
        }
    }
}

impl Default for ServiceRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_service(service_id: &str) -> ServiceInfo {
        ServiceInfo {
            service_id: service_id.to_string(),
            endpoint: format!("http://{}.local:8080", service_id),
            handlers: vec![HandlerInfo {
                actor_name: "TestActor".to_string(),
                operation: "test_operation".to_string(),
                mode: OperationMode::Exclusive,
                metadata: HashMap::new(),
            }],
            status: (WorkerStatus::Healthy as i32),
            registered_at: Timestamptz::now(),
            last_heartbeat: Timestamptz::now(),
            metadata: HashMap::new(),
            metrics: None,
        }
    }

    #[tokio::test]
    async fn test_register_service() {
        let registry = ServiceRegistry::new();
        let service = create_test_service("order-service-1");

        let registration_id = registry.register(service).await.unwrap();
        assert!(!registration_id.is_empty());

        let count = registry.service_count().await;
        assert_eq!(count, 1);
    }

    #[tokio::test]
    async fn test_find_services() {
        let registry = ServiceRegistry::new();
        let service = create_test_service("order-service-1");
        registry.register(service).await.unwrap();

        let services = registry.find_services("TestActor", "test_operation").await;
        assert_eq!(services.len(), 1);
        assert_eq!(services[0].service_id, "order-service-1");
    }

    #[tokio::test]
    async fn test_has_handler() {
        let registry = ServiceRegistry::new();

        // Unknown operation before any registration.
        assert!(!registry.has_handler("TestActor", "test_operation").await);

        registry
            .register(create_test_service("order-service-1"))
            .await
            .unwrap();

        // Registered operation is now known; unrelated ones are not.
        assert!(registry.has_handler("TestActor", "test_operation").await);
        assert!(!registry.has_handler("TestActor", "other_operation").await);
        assert!(!registry.has_handler("OtherActor", "test_operation").await);
    }

    #[tokio::test]
    async fn test_has_handler_false_after_unregister() {
        let registry = ServiceRegistry::new();
        registry
            .register(create_test_service("order-service-1"))
            .await
            .unwrap();
        assert!(registry.has_handler("TestActor", "test_operation").await);

        registry.unregister("order-service-1").await.unwrap();
        // The handler index entry is dropped when its last service leaves.
        assert!(!registry.has_handler("TestActor", "test_operation").await);
    }

    #[tokio::test]
    async fn test_unregister_service() {
        let registry = ServiceRegistry::new();
        let service = create_test_service("order-service-1");
        registry.register(service).await.unwrap();

        let removed = registry.unregister("order-service-1").await.unwrap();
        assert!(removed);

        let count = registry.service_count().await;
        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn test_heartbeat() {
        let registry = ServiceRegistry::new();
        let mut service = create_test_service("order-service-1");
        service.last_heartbeat = Timestamptz::now() - SignedDuration::from_mins(5);
        registry.register(service).await.unwrap();

        let updated = registry
            .heartbeat("order-service-1", WorkerStatus::Healthy as i32, None)
            .await
            .unwrap();
        assert!(updated);

        let service_info = registry.get_service("order-service-1").await.unwrap();
        assert!(service_info.is_healthy(SignedDuration::from_secs(30)));
    }

    #[tokio::test]
    async fn test_cleanup_stale_services() {
        let registry = ServiceRegistry::new_with_timeout(SignedDuration::from_secs(5));

        let mut service = create_test_service("order-service-1");
        service.last_heartbeat = Timestamptz::now() - SignedDuration::from_secs(20); // Very stale
        registry.register(service).await.unwrap();

        let removed = registry.cleanup_stale_services().await;
        assert_eq!(removed, 1);

        let count = registry.service_count().await;
        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn cleanup_stale_services_clears_the_handler_index() {
        // Reaping a dead service must also drop its handler-index entry, so
        // that operation-mode resolution and InvokeOperation's fast-fail see
        // only the live fleet — not just the service count.
        let registry = ServiceRegistry::new_with_timeout(SignedDuration::from_secs(5));

        let mut service = create_test_service("order-service-1");
        service.last_heartbeat = Timestamptz::now() - SignedDuration::from_secs(20);
        registry.register(service).await.unwrap();
        assert!(registry.has_handler("TestActor", "test_operation").await);

        let removed = registry.cleanup_stale_services().await;
        assert_eq!(removed, 1);
        assert!(!registry.has_handler("TestActor", "test_operation").await);
    }

    #[test]
    fn test_service_is_healthy() {
        let mut service = create_test_service("order-service-1");
        assert!(service.is_healthy(SignedDuration::from_secs(30)));

        service.last_heartbeat = Timestamptz::now() - SignedDuration::from_mins(5);
        assert!(!service.is_healthy(SignedDuration::from_secs(30)));
    }

    #[test]
    fn test_service_can_handle() {
        let service = create_test_service("order-service-1");
        assert!(service.can_handle("TestActor", "test_operation"));
        assert!(!service.can_handle("OtherActor", "test_operation"));
        assert!(!service.can_handle("TestActor", "other_operation"));
    }

    fn make_service_with_metrics(id: &str, ops: u32, cpu: f64) -> ServiceInfo {
        let mut s = create_test_service(id);
        s.metrics = Some(WorkerMetrics {
            operations_in_progress: ops,
            cpu_usage: cpu,
            ..Default::default()
        });
        s
    }

    #[test]
    fn test_load_score_ordering() {
        let idle = make_service_with_metrics("a", 0, 5.0);
        let busy = make_service_with_metrics("b", 3, 10.0);
        let no_metrics = create_test_service("c");

        assert!(idle.load_score() < busy.load_score());
        assert!(busy.load_score() < no_metrics.load_score());
    }

    #[tokio::test]
    async fn test_select_service_picks_least_loaded() {
        let registry = ServiceRegistry::new();

        let idle = make_service_with_metrics("idle-svc", 0, 5.0);
        let busy = make_service_with_metrics("busy-svc", 5, 80.0);

        registry.register(idle).await.unwrap();
        registry.register(busy).await.unwrap();

        // Should always pick the idle service
        for _ in 0..5 {
            let selected = registry
                .select_service("TestActor", "test_operation")
                .await
                .unwrap();
            assert_eq!(selected.service_id, "idle-svc");
        }
    }

    fn create_test_service_with_mode(service_id: &str, mode: OperationMode) -> ServiceInfo {
        let mut service = create_test_service(service_id);
        service.handlers[0].mode = mode;
        service
    }

    #[tokio::test]
    async fn resolve_mode_unknown_operation_is_exclusive() {
        let registry = ServiceRegistry::new();
        assert_eq!(
            registry.resolve_operation_mode("NoSuchActor", "nope").await,
            OperationMode::Exclusive
        );
    }

    #[tokio::test]
    async fn resolve_mode_unanimous_shared_is_shared() {
        let registry = ServiceRegistry::new();
        registry
            .register(create_test_service_with_mode(
                "svc-1",
                OperationMode::Shared,
            ))
            .await
            .unwrap();
        registry
            .register(create_test_service_with_mode(
                "svc-2",
                OperationMode::Shared,
            ))
            .await
            .unwrap();

        assert_eq!(
            registry
                .resolve_operation_mode("TestActor", "test_operation")
                .await,
            OperationMode::Shared
        );
    }

    #[tokio::test]
    async fn resolve_mode_mixed_fleet_degrades_to_exclusive() {
        // A rolling deploy where one worker still declares the operation
        // exclusive must degrade the whole fleet to exclusive.
        let registry = ServiceRegistry::new();
        registry
            .register(create_test_service_with_mode(
                "svc-old",
                OperationMode::Exclusive,
            ))
            .await
            .unwrap();
        registry
            .register(create_test_service_with_mode(
                "svc-new",
                OperationMode::Shared,
            ))
            .await
            .unwrap();

        assert_eq!(
            registry
                .resolve_operation_mode("TestActor", "test_operation")
                .await,
            OperationMode::Exclusive
        );
    }

    #[tokio::test]
    async fn test_select_service_round_robins_on_tie() {
        let registry = ServiceRegistry::new();

        // Both services equally idle
        let a = make_service_with_metrics("svc-a", 0, 0.0);
        let b = make_service_with_metrics("svc-b", 0, 0.0);

        registry.register(a).await.unwrap();
        registry.register(b).await.unwrap();

        let mut seen_a = false;
        let mut seen_b = false;
        for _ in 0..10 {
            let selected = registry
                .select_service("TestActor", "test_operation")
                .await
                .unwrap();
            if selected.service_id == "svc-a" {
                seen_a = true;
            } else {
                seen_b = true;
            }
        }
        assert!(
            seen_a && seen_b,
            "Round-robin should distribute across tied services"
        );
    }
}
