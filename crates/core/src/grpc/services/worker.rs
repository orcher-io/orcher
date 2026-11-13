//! Worker registration gRPC service.

use crate::grpc::tenant::organization_from_metadata;
use async_trait::async_trait;
use dashmap::DashMap;
use orcher_common::types::identifiers::OrganizationId;
use orcher_proto::orcher::v1::{
    worker_service_server::WorkerService, DeregisterWorkerRequest, DeregisterWorkerResponse,
    RegisterWorkerRequest, RegisterWorkerResponse, WorkerHeartbeatRequest, WorkerHeartbeatResponse,
    WorkerRegistrationStatus,
};
use orcher_storage::repositories::domains::worker::WorkerRepository;
use orcher_storage::schema::types::worker::Worker;
use std::sync::Arc;
use tonic::{Request, Response, Status};
use tracing::{debug, error, info, warn};

use crate::worker::registry::{WorkerCapabilities, WorkerInfo, WorkerRegistry};
use orcher_common::types::time::Timestamptz;

/// How often to persist heartbeats to DB (every Nth heartbeat).
/// At a 5s heartbeat interval, 6 means ~30s between DB writes per worker.
const HEARTBEAT_DB_INTERVAL: u64 = 6;

/// Default organization ID for SDK workers that don't send org_id.
/// This is a well-known UUID used until multi-tenancy is wired into the SDK.
fn default_organization_id() -> OrganizationId {
    OrganizationId::from_uuid(uuid::Uuid::nil())
}

/// Worker service gRPC implementation
///
/// Handles worker registration, heartbeat, and deregistration RPCs.
/// Optionally persists state to PostgreSQL via `WorkerRepository`.
#[derive(Clone)]
pub struct WorkerServiceImpl {
    /// In-memory worker registry (primary, low-latency)
    registry: Arc<WorkerRegistry>,

    /// Optional DB repository for write-through persistence
    worker_repo: Option<WorkerRepository>,

    /// Per-worker heartbeat counter for DB write throttling
    heartbeat_counter: Arc<DashMap<String, u64>>,
}

impl WorkerServiceImpl {
    /// Create a new worker service implementation (in-memory only)
    pub fn new(registry: Arc<WorkerRegistry>) -> Self {
        info!("Initializing WorkerServiceImpl");
        Self {
            registry,
            worker_repo: None,
            heartbeat_counter: Arc::new(DashMap::new()),
        }
    }

    /// Add DB persistence via WorkerRepository (write-through pattern)
    pub fn with_worker_repository(mut self, repo: WorkerRepository) -> Self {
        info!("WorkerServiceImpl: DB persistence enabled via WorkerRepository");
        self.worker_repo = Some(repo);
        self
    }

    /// Create a gRPC server for this service
    pub fn into_server(
        self,
    ) -> orcher_proto::orcher::v1::worker_service_server::WorkerServiceServer<Self> {
        orcher_proto::orcher::v1::worker_service_server::WorkerServiceServer::new(self)
    }

    /// Get reference to registry
    pub fn registry(&self) -> &Arc<WorkerRegistry> {
        &self.registry
    }

    /// Persist worker registration to DB (non-fatal on error)
    async fn persist_registration(&self, info: &WorkerInfo) {
        let Some(repo) = &self.worker_repo else {
            return;
        };

        let worker = worker_info_to_entity(info);

        if let Err(e) = repo.upsert(&worker).await {
            warn!(
                service_id = %info.service_id,
                error = %e,
                "Failed to persist worker registration to DB (non-fatal)"
            );
        } else {
            debug!(
                service_id = %info.service_id,
                "Worker registration persisted to DB"
            );
        }
    }

    /// Persist heartbeat to DB with throttling (non-fatal on error)
    async fn persist_heartbeat(&self, service_id: &str, status: &str, tasks_in_progress: i32) {
        let Some(repo) = &self.worker_repo else {
            return;
        };

        // Increment and check counter
        let count = {
            let mut entry = self
                .heartbeat_counter
                .entry(service_id.to_string())
                .or_insert(0);
            *entry += 1;
            *entry
        };

        if count % HEARTBEAT_DB_INTERVAL != 0 {
            return;
        }

        if let Err(e) = repo
            .update_heartbeat_by_worker_id(service_id, status, tasks_in_progress)
            .await
        {
            warn!(
                service_id = %service_id,
                error = %e,
                "Failed to persist heartbeat to DB (non-fatal)"
            );
        }
    }

    /// Whether `caller` is entitled to act on the worker registered as
    /// `service_id`.
    ///
    /// `register_worker` records the organisation a worker belongs to, but
    /// heartbeat and deregistration took a `service_id` and acted on whatever
    /// worker bore that name. A service id is not a secret — it appears in
    /// worker listings and logs — so this let one tenant deregister another's
    /// workers, leaving their task queues with no pollers and every execution
    /// on them stalled, or forge heartbeats to keep a dead worker looking alive
    /// so the reaper never reclaimed its tasks.
    ///
    /// A worker the registry has never seen is allowed through: that is the
    /// "unknown worker, please re-register" path, which is a liveness signal
    /// rather than an authorisation decision and is handled by the caller.
    async fn may_act_on_worker(&self, service_id: &str, caller: Option<uuid::Uuid>) -> bool {
        let Some(worker) = self.registry.get_worker(service_id).await else {
            return true;
        };

        // A worker stored under the nil organisation registered before there
        // was a header to stamp, so it is unowned rather than owned by a
        // tenant named all-zeroes. `may_act_on` refuses unowned rows to a
        // caller acting for an organisation, which is the conservative reading
        // and the same one the schedule and workflow guards take.
        let owner = worker.organization_id.filter(|id| !id.is_nil());

        crate::grpc::tenant::may_act_on(owner, caller)
    }

    /// Take on a worker another replica registered, from its persisted row.
    ///
    /// `true` when it was adopted and heartbeats can be accepted here. Refused
    /// for a worker that deregistered (its row is offline), and for a caller
    /// that may not act on the row's owner — the in-memory ownership check
    /// only covers workers this replica already knows.
    async fn adopt_persisted_worker(&self, service_id: &str, caller: Option<uuid::Uuid>) -> bool {
        let Some(repo) = &self.worker_repo else {
            return false;
        };
        let worker = match repo.find_by_worker_id(service_id).await {
            Ok(Some(worker)) => worker,
            Ok(None) => return false,
            Err(e) => {
                warn!(service_id = %service_id, error = %e, "Could not look up a persisted worker registration");
                return false;
            }
        };
        if worker.status == orcher_storage::schema::types::worker::WorkerStatus::Offline {
            return false;
        }
        let owner = Some(worker.organization_id.as_uuid()).filter(|id| !id.is_nil());
        if !crate::grpc::tenant::may_act_on(owner, caller) {
            return false;
        }
        if self
            .registry
            .register(worker_info_from_entity(&worker))
            .await
            .is_err()
        {
            return false;
        }
        info!(
            service_id = %service_id,
            "Adopted a worker registered on another replica"
        );
        true
    }

    /// Persist deregistration to DB (non-fatal on error)
    async fn persist_deregistration(&self, service_id: &str) {
        let Some(repo) = &self.worker_repo else {
            return;
        };

        // Clean up heartbeat counter
        self.heartbeat_counter.remove(service_id);

        if let Err(e) = repo.set_offline_by_worker_id(service_id).await {
            warn!(
                service_id = %service_id,
                error = %e,
                "Failed to persist worker deregistration to DB (non-fatal)"
            );
        } else {
            debug!(
                service_id = %service_id,
                "Worker deregistration persisted to DB (status → offline)"
            );
        }
    }
}

#[async_trait]
impl WorkerService for WorkerServiceImpl {
    /// Register a worker and its capabilities
    async fn register_worker(
        &self,
        request: Request<RegisterWorkerRequest>,
    ) -> Result<Response<RegisterWorkerResponse>, Status> {
        // The auth layer has already verified the caller and stamped the
        // organisation it belongs to; record the worker under it.
        let organization_id = organization_from_metadata(request.metadata());
        let req = request.into_inner();

        info!(
            service_id = %req.service_id,
            identity = %req.identity,
            task_queue = %req.task_queue,
            namespace = %req.namespace,
            "RegisterWorker request"
        );

        let capabilities = req.capabilities.unwrap_or_default();

        let worker_info = WorkerInfo {
            service_id: req.service_id.clone(),
            identity: req.identity,
            task_queue: req.task_queue,
            namespace: req.namespace,
            capabilities: WorkerCapabilities {
                workflow_types: capabilities.workflow_types,
                task_types: capabilities.task_types,
                max_concurrent_workflows: capabilities.max_concurrent_workflows,
                max_concurrent_tasks: capabilities.max_concurrent_tasks,
            },
            status: WorkerRegistrationStatus::Healthy as i32,
            registered_at: Timestamptz::now(),
            last_heartbeat: Timestamptz::now(),
            metadata: req.metadata,
            metrics: None,
            organization_id,
        };

        match self.registry.register(worker_info.clone()).await {
            Ok(registration_id) => {
                info!(
                    service_id = %req.service_id,
                    registration_id = %registration_id,
                    "Worker registered successfully"
                );

                // Reset heartbeat counter on (re-)registration
                self.heartbeat_counter.remove(&req.service_id);

                // Write-through to DB (non-fatal)
                self.persist_registration(&worker_info).await;

                Ok(Response::new(RegisterWorkerResponse {
                    success: true,
                    registration_id,
                    error_message: String::new(),
                }))
            }
            Err(e) => {
                error!(
                    service_id = %req.service_id,
                    error = %e,
                    "Failed to register worker"
                );

                Ok(Response::new(RegisterWorkerResponse {
                    success: false,
                    registration_id: String::new(),
                    error_message: format!("Failed to register worker: {}", e),
                }))
            }
        }
    }

    /// Process worker heartbeat
    async fn worker_heartbeat(
        &self,
        request: Request<WorkerHeartbeatRequest>,
    ) -> Result<Response<WorkerHeartbeatResponse>, Status> {
        // Before `into_inner()`, which drops the metadata the tenant lives in.
        let caller = organization_from_metadata(request.metadata());
        let req = request.into_inner();

        debug!(
            service_id = %req.service_id,
            registration_id = %req.registration_id,
            "WorkerHeartbeat"
        );

        if !self.may_act_on_worker(&req.service_id, caller).await {
            // Answered as "I do not know this worker, re-register" rather than
            // with an error, because that is the reply this RPC already has for
            // a worker it cannot place, and the SDK knows how to act on it. It
            // also declines to confirm that the service id names a real worker
            // somewhere else.
            warn!(
                service_id = %req.service_id,
                "Heartbeat for a worker belonging to another organisation"
            );

            return Ok(Response::new(WorkerHeartbeatResponse {
                success: false,
                re_register: true,
                timestamp: Timestamptz::now().timestamp(),
            }));
        }

        let tasks_in_progress = req
            .metrics
            .as_ref()
            .map(|m| m.tasks_in_progress as i32)
            .unwrap_or(0);
        let workflows_in_progress = req
            .metrics
            .as_ref()
            .map(|m| m.workflows_in_progress as i32)
            .unwrap_or(0);
        let db_status = heartbeat_status(req.status, tasks_in_progress + workflows_in_progress);

        match self
            .registry
            .heartbeat(&req.service_id, req.status, req.metrics)
            .await
        {
            Ok(true) => {
                // Write-through to DB with throttling (non-fatal)
                self.persist_heartbeat(&req.service_id, db_status, tasks_in_progress)
                    .await;

                Ok(Response::new(WorkerHeartbeatResponse {
                    success: true,
                    re_register: false,
                    timestamp: Timestamptz::now().timestamp(),
                }))
            }
            // Unknown here may only mean registered on another replica: the
            // registry is per replica, and a worker's calls are not guaranteed
            // to reach the one it registered with. Its persisted registration
            // is shared, so adopt it instead of making the worker re-register
            // — which, with calls alternating between replicas, it did on
            // every heartbeat.
            Ok(false) if self.adopt_persisted_worker(&req.service_id, caller).await => {
                let _ = self
                    .registry
                    .heartbeat(&req.service_id, req.status, req.metrics)
                    .await;
                self.persist_heartbeat(&req.service_id, db_status, tasks_in_progress)
                    .await;

                Ok(Response::new(WorkerHeartbeatResponse {
                    success: true,
                    re_register: false,
                    timestamp: Timestamptz::now().timestamp(),
                }))
            }
            Ok(false) => {
                warn!(
                    service_id = %req.service_id,
                    "Heartbeat from unknown worker — requesting re-registration"
                );

                Ok(Response::new(WorkerHeartbeatResponse {
                    success: false,
                    re_register: true,
                    timestamp: Timestamptz::now().timestamp(),
                }))
            }
            Err(e) => {
                error!(
                    service_id = %req.service_id,
                    error = %e,
                    "Failed to process heartbeat"
                );

                Err(Status::internal(format!(
                    "Failed to process heartbeat: {}",
                    e
                )))
            }
        }
    }

    /// Deregister a worker (graceful shutdown)
    async fn deregister_worker(
        &self,
        request: Request<DeregisterWorkerRequest>,
    ) -> Result<Response<DeregisterWorkerResponse>, Status> {
        // Before `into_inner()`, which drops the metadata the tenant lives in.
        let caller = organization_from_metadata(request.metadata());
        let req = request.into_inner();

        info!(
            service_id = %req.service_id,
            registration_id = %req.registration_id,
            "DeregisterWorker request"
        );

        if !self.may_act_on_worker(&req.service_id, caller).await {
            warn!(
                service_id = %req.service_id,
                "Refused deregistration of a worker belonging to another organisation"
            );

            // Not-found, not permission-denied: a caller who may not touch this
            // worker should not learn from the reply that it exists.
            return Err(Status::not_found("Worker not found"));
        }

        match self.registry.deregister(&req.service_id).await {
            Ok(_) => {
                // Write-through to DB (non-fatal)
                self.persist_deregistration(&req.service_id).await;

                Ok(Response::new(DeregisterWorkerResponse { success: true }))
            }
            Err(e) => {
                error!(
                    service_id = %req.service_id,
                    error = %e,
                    "Failed to deregister worker"
                );

                Err(Status::internal(format!(
                    "Failed to deregister worker: {}",
                    e
                )))
            }
        }
    }
}

/// Convert proto WorkerRegistrationStatus (i32) to database status string
/// The `workers.status` a heartbeat implies.
///
/// A worker reports its own condition, not its load, and every SDK reports
/// `Healthy` always — which used to be written as `idle` whatever the worker
/// was doing, so the column never once said `busy`. Load decides between idle
/// and busy; the worker's own word only matters when it is draining or in
/// trouble.
fn heartbeat_status(status: i32, in_progress: i32) -> &'static str {
    match status {
        s if s == WorkerRegistrationStatus::Draining as i32 => "draining",
        s if s == WorkerRegistrationStatus::Unhealthy as i32 => "error",
        _ if in_progress > 0 => "busy",
        _ => "idle",
    }
}

#[allow(dead_code)]
fn proto_status_to_db_status(status: i32) -> &'static str {
    match status {
        s if s == WorkerRegistrationStatus::Healthy as i32 => "idle",
        s if s == WorkerRegistrationStatus::Degraded as i32 => "busy",
        s if s == WorkerRegistrationStatus::Draining as i32 => "draining",
        s if s == WorkerRegistrationStatus::Unhealthy as i32 => "error",
        _ => "idle",
    }
}

/// Convert in-memory WorkerInfo to storage Worker entity for DB persistence
/// The registration a replica needs, rebuilt from a persisted worker row —
/// the inverse of [`worker_info_to_entity`].
fn worker_info_from_entity(worker: &Worker) -> WorkerInfo {
    let text = |key: &str| {
        worker
            .metadata
            .get(key)
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string()
    };
    let with_prefix = |prefix: &str| {
        worker
            .capabilities
            .iter()
            .filter_map(|c| c.strip_prefix(prefix).map(str::to_string))
            .collect::<Vec<_>>()
    };
    let metadata = worker
        .metadata
        .get("sdk_metadata")
        .and_then(|v| v.as_object())
        .map(|m| {
            m.iter()
                .filter_map(|(k, v)| v.as_str().map(|v| (k.clone(), v.to_string())))
                .collect()
        })
        .unwrap_or_default();
    let max_tasks = worker.max_concurrent_tasks.max(0) as u32;
    WorkerInfo {
        service_id: worker.worker_id.clone(),
        identity: text("identity"),
        task_queue: text("task_queue"),
        namespace: text("namespace"),
        capabilities: WorkerCapabilities {
            workflow_types: with_prefix("workflow:"),
            task_types: with_prefix("task:"),
            // Not persisted; the task limit is the closest recorded bound.
            max_concurrent_workflows: max_tasks,
            max_concurrent_tasks: max_tasks,
        },
        status: orcher_proto::orcher::v1::WorkerRegistrationStatus::Healthy as i32,
        registered_at: worker.registered_at.unwrap_or_else(Timestamptz::now),
        last_heartbeat: Timestamptz::now(),
        metadata,
        metrics: None,
        organization_id: Some(worker.organization_id.as_uuid()).filter(|id| !id.is_nil()),
    }
}

fn worker_info_to_entity(info: &WorkerInfo) -> Worker {
    let mut capabilities: Vec<String> = Vec::new();
    for wf in &info.capabilities.workflow_types {
        capabilities.push(format!("workflow:{}", wf));
    }
    for task in &info.capabilities.task_types {
        capabilities.push(format!("task:{}", task));
    }

    let metadata = serde_json::json!({
        "task_queue": info.task_queue,
        "namespace": info.namespace,
        "identity": info.identity,
        "sdk_metadata": info.metadata,
    });

    let host_address = info
        .metadata
        .get("host")
        .cloned()
        .unwrap_or_else(|| "unknown".to_string());

    let port = info
        .metadata
        .get("port")
        .and_then(|p| p.parse::<i32>().ok())
        .unwrap_or(0);

    Worker::new(
        info.organization_id
            .map(OrganizationId::from_uuid)
            .unwrap_or_else(default_organization_id),
        info.service_id.clone(),
        info.identity.clone(),
        host_address,
        port,
    )
    .with_capabilities(capabilities)
    .with_metadata(metadata)
    .with_worker_type("sdk-worker".to_string())
    .with_max_concurrent_tasks(info.capabilities.max_concurrent_tasks as i32)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn info(organization_id: Option<uuid::Uuid>) -> WorkerInfo {
        WorkerInfo {
            service_id: "svc@pod-1".to_string(),
            identity: "svc@pod-1".to_string(),
            task_queue: "default".to_string(),
            namespace: "payments-staging".to_string(),
            capabilities: WorkerCapabilities {
                workflow_types: vec![],
                task_types: vec![],
                max_concurrent_workflows: 1,
                max_concurrent_tasks: 1,
            },
            status: WorkerRegistrationStatus::Healthy as i32,
            registered_at: Timestamptz::now(),
            last_heartbeat: Timestamptz::now(),
            metadata: HashMap::new(),
            metrics: None,
            organization_id,
        }
    }

    /// Two replicas share one database but keep separate registries, and a
    /// worker's calls are not guaranteed to reach the one it registered with.
    /// A heartbeat to the other replica was answered "re-register", which
    /// with calls alternating between replicas happened on every heartbeat.
    /// It must be accepted there from the persisted registration.
    #[tokio::test]
    #[ignore] // Requires PostgreSQL
    async fn a_heartbeat_reaches_a_replica_the_worker_did_not_register_with() {
        use orcher_proto::orcher::v1::worker_service_server::WorkerService as _;
        use orcher_proto::orcher::v1::{RegisterWorkerRequest, WorkerHeartbeatRequest};

        let url = std::env::var("TEST_DATABASE_URL").unwrap_or_else(|_| {
            "postgresql://postgres:postgres@localhost:5432/orcher_test".to_string()
        });
        let pool = sqlx::PgPool::connect(&url)
            .await
            .expect("connect to the test database");
        let replica = || {
            WorkerServiceImpl::new(Arc::new(WorkerRegistry::new()))
                .with_worker_repository(WorkerRepository::new(pool.clone()))
        };
        let (first, second) = (replica(), replica());
        let service_id = format!("svc@{}", uuid::Uuid::new_v4());

        let registered = first
            .register_worker(Request::new(RegisterWorkerRequest {
                service_id: service_id.clone(),
                identity: service_id.clone(),
                task_queue: "transfers".to_string(),
                namespace: "default".to_string(),
                ..Default::default()
            }))
            .await
            .expect("register with the first replica")
            .into_inner();

        let heartbeat = second
            .worker_heartbeat(Request::new(WorkerHeartbeatRequest {
                service_id: service_id.clone(),
                registration_id: registered.registration_id.clone(),
                ..Default::default()
            }))
            .await
            .expect("heartbeat to the second replica")
            .into_inner();
        assert!(
            heartbeat.success,
            "the second replica accepted the heartbeat"
        );
        assert!(
            !heartbeat.re_register,
            "and did not ask the worker to re-register"
        );

        // A worker that deregistered is not brought back.
        first
            .deregister_worker(Request::new(
                orcher_proto::orcher::v1::DeregisterWorkerRequest {
                    service_id: service_id.clone(),
                    registration_id: registered.registration_id,
                },
            ))
            .await
            .expect("deregister");
        let third = replica();
        let after = third
            .worker_heartbeat(Request::new(WorkerHeartbeatRequest {
                service_id: service_id.clone(),
                ..Default::default()
            }))
            .await
            .expect("heartbeat after deregistering")
            .into_inner();
        assert!(
            after.re_register,
            "a deregistered worker is asked to register again"
        );

        let _ = sqlx::query("DELETE FROM workers WHERE worker_id = $1")
            .bind(&service_id)
            .execute(&pool)
            .await;
    }

    async fn service_with_worker(organization_id: Option<uuid::Uuid>) -> WorkerServiceImpl {
        let registry = Arc::new(WorkerRegistry::new());
        registry
            .register(info(organization_id))
            .await
            .expect("register");
        WorkerServiceImpl::new(registry)
    }

    #[tokio::test]
    async fn an_organisation_may_act_on_its_own_worker() {
        let mine = uuid::Uuid::new_v4();
        let service = service_with_worker(Some(mine)).await;

        assert!(service.may_act_on_worker("svc@pod-1", Some(mine)).await);
    }

    /// The hole this closes: a service id is not a secret — it is in worker
    /// listings and log lines — and it was the only thing deregistration
    /// checked, so one tenant could take another's workers offline and leave
    /// their queues with nothing polling them.
    #[tokio::test]
    async fn another_organisation_may_not_act_on_a_worker() {
        let service = service_with_worker(Some(uuid::Uuid::new_v4())).await;

        assert!(
            !service
                .may_act_on_worker("svc@pod-1", Some(uuid::Uuid::new_v4()))
                .await
        );
    }

    /// A worker registered before there was a header to stamp is stored under
    /// the nil organisation. That is "unowned", not "owned by all-zeroes", so
    /// it is refused to a tenant rather than belonging to whoever sends the nil
    /// UUID.
    #[tokio::test]
    async fn a_worker_stored_under_the_nil_organisation_is_unowned() {
        let service = service_with_worker(Some(uuid::Uuid::nil())).await;

        assert!(
            !service
                .may_act_on_worker("svc@pod-1", Some(uuid::Uuid::new_v4()))
                .await
        );
        assert!(service.may_act_on_worker("svc@pod-1", None).await);
    }

    /// A single-tenant deployment sends no organisation and must keep working.
    #[tokio::test]
    async fn a_deployment_without_tenancy_may_act_on_its_workers() {
        let service = service_with_worker(Some(uuid::Uuid::new_v4())).await;

        assert!(service.may_act_on_worker("svc@pod-1", None).await);
    }

    /// An unregistered service id is left alone: "I do not know this worker" is
    /// the re-registration path, which is a liveness answer rather than an
    /// authorisation one, and stealing it here would turn a restarted worker
    /// into a permission error.
    #[tokio::test]
    async fn an_unknown_worker_is_left_to_the_re_registration_path() {
        let service = service_with_worker(Some(uuid::Uuid::new_v4())).await;

        assert!(
            service
                .may_act_on_worker("svc@never-registered", Some(uuid::Uuid::new_v4()))
                .await
        );
    }

    /// Every SDK reports `Healthy` on every heartbeat, so the worker's own
    /// word cannot distinguish idle from busy: load does.
    #[test]
    fn a_heartbeat_with_work_in_progress_is_busy() {
        let healthy = WorkerRegistrationStatus::Healthy as i32;
        assert_eq!(heartbeat_status(healthy, 0), "idle");
        assert_eq!(heartbeat_status(healthy, 1), "busy");
        assert_eq!(heartbeat_status(healthy, 42), "busy");
    }

    /// Draining and failing are the worker's own condition and outrank load.
    #[test]
    fn a_worker_in_trouble_reports_its_own_state() {
        assert_eq!(
            heartbeat_status(WorkerRegistrationStatus::Draining as i32, 3),
            "draining"
        );
        assert_eq!(
            heartbeat_status(WorkerRegistrationStatus::Unhealthy as i32, 3),
            "error"
        );
    }

    /// The row must carry the caller's organisation, or the tenant's worker
    /// list never shows the worker.
    #[test]
    fn the_worker_row_is_stamped_with_the_callers_organisation() {
        let org = uuid::Uuid::new_v4();
        let worker = worker_info_to_entity(&info(Some(org)));
        assert_eq!(worker.organization_id.as_uuid(), org);
    }

    #[test]
    fn without_tenancy_the_row_falls_back_to_the_nil_organisation() {
        let worker = worker_info_to_entity(&info(None));
        assert_eq!(worker.organization_id.as_uuid(), uuid::Uuid::nil());
    }
}
