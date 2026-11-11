//! Actor operations gRPC service.

use async_trait::async_trait;
use orcher_proto::orcher::v1::{
    actor_service_server::ActorService, CompleteActorOperationRequest,
    CompleteActorOperationResponse, DeleteStateRequest, DeleteStateResponse, ExecutionStatus,
    GetStateRequest, GetStateResponse, HeartbeatRequest, HeartbeatResponse, InvokeOperationRequest,
    InvokeOperationResponse, ListStateKeysRequest, ListStateKeysResponse, OperationMode,
    PollActorOperationRequest, PollActorOperationResponse, RegisterHandlersRequest,
    RegisterHandlersResponse, SetStateRequest, SetStateResponse, WorkerStatus,
};
use std::sync::Arc;
use tonic::{Request, Response, Status};
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use crate::actor::router::ActorRouter;
use crate::actor::service_registry::{HandlerInfo, ServiceInfo, ServiceRegistry};
use crate::actor::state_storage::ActorStateStorage;
use crate::grpc::service_auth::{ServiceIdentity, ServiceOperation};
use orcher_common::types::time::Timestamptz;

/// Actor service gRPC implementation
///
/// Handles all RPC methods for the actor system including:
/// - Operation invocation (client → server)
/// - State access (service → server)
/// - Handler registration and heartbeat (service lifecycle)
///
/// Note: Using ORCHER terminology - "Service" not "Worker"
#[derive(Clone)]
pub struct ActorServiceImpl {
    /// Actor router for operation routing and single-writer enforcement
    router: Arc<ActorRouter>,

    /// Actor state storage for state operations
    storage: Arc<ActorStateStorage>,

    /// Service registry for handler tracking
    registry: Arc<ServiceRegistry>,
}

impl ActorServiceImpl {
    /// Create a new actor service implementation
    pub fn new(
        router: Arc<ActorRouter>,
        storage: Arc<ActorStateStorage>,
        registry: Arc<ServiceRegistry>,
    ) -> Self {
        info!("Initializing ActorServiceImpl");
        Self {
            router,
            storage,
            registry,
        }
    }

    /// Create a gRPC server for this service
    ///
    /// This method follows the Orcher pattern for converting service implementations
    /// into tonic gRPC servers.
    pub fn into_server(
        self,
    ) -> orcher_proto::orcher::v1::actor_service_server::ActorServiceServer<Self> {
        orcher_proto::orcher::v1::actor_service_server::ActorServiceServer::new(self)
    }

    /// Get reference to router
    pub fn router(&self) -> &Arc<ActorRouter> {
        &self.router
    }

    /// Get reference to storage
    pub fn storage(&self) -> &Arc<ActorStateStorage> {
        &self.storage
    }

    /// Get reference to registry
    pub fn registry(&self) -> &Arc<ServiceRegistry> {
        &self.registry
    }
}

/// Check an authenticated caller's permission for an operation, as the task
/// RPCs do. Actor requests carry no namespace, so there is none to check; the
/// organization is checked against the operation itself. A request with no
/// authenticated identity is a deployment without authentication.
#[allow(clippy::result_large_err)]
fn authorize<T>(request: &Request<T>, operation: ServiceOperation) -> Result<(), Status> {
    if let Some(identity) = request.extensions().get::<ServiceIdentity>() {
        crate::grpc::auth::check_operation(identity, operation)?;
    }
    Ok(())
}

#[async_trait]
impl ActorService for ActorServiceImpl {
    /// Poll for actor operations (service → server)
    ///
    /// Services call this to poll for available work. This is a long-poll
    /// that blocks until work is available or timeout.
    async fn poll_actor_operation(
        &self,
        request: Request<PollActorOperationRequest>,
    ) -> Result<Response<PollActorOperationResponse>, Status> {
        authorize(&request, ServiceOperation::PollActivityTasks)?;
        // Before `into_inner()`, which drops the metadata the tenant lives in.
        let caller = crate::grpc::tenant::organization_from_metadata(request.metadata());
        let req = request.into_inner();

        debug!(
            "PollActorOperation: service_id={}, max_operations={}, timeout_ms={}",
            req.service_id, req.max_operations, req.timeout_ms
        );

        let timeout = std::time::Duration::from_millis(req.timeout_ms);
        let max_operations = req.max_operations.clamp(1, 100) as usize; // Cap at 100

        // Capability-aware dispatch: a worker only receives operations for the
        // handlers it registered. Dispatch is otherwise a single global queue,
        // so without this filter a worker in a fleet running a different actor
        // catalog would be handed an operation it cannot execute and reject it.
        // A worker that has not registered (unknown service_id) can handle
        // nothing and simply long-polls until its registration lands.
        let service = self.registry.get_service(&req.service_id).await;
        let can_handle = |actor_name: &str, operation: &str| {
            service
                .as_ref()
                .map(|s| s.can_handle(actor_name, operation))
                .unwrap_or(false)
        };

        match self
            .router
            .poll_operations_for(caller, &req.service_id, max_operations, timeout, can_handle)
            .await
        {
            Ok(operations) => {
                if !operations.is_empty() {
                    info!(
                        "Dispatched operations: service_id={}, count={}",
                        req.service_id,
                        operations.len()
                    );
                } else {
                    debug!("Poll timeout: service_id={}", req.service_id);
                }

                Ok(Response::new(PollActorOperationResponse { operations }))
            }
            Err(e) => {
                error!("Poll failed: service_id={}, error={}", req.service_id, e);
                Err(Status::internal(format!(
                    "Failed to poll operations: {}",
                    e
                )))
            }
        }
    }

    /// Complete an actor operation (service → server)
    ///
    /// Services call this after executing an operation to report the result.
    async fn complete_actor_operation(
        &self,
        request: Request<CompleteActorOperationRequest>,
    ) -> Result<Response<CompleteActorOperationResponse>, Status> {
        authorize(&request, ServiceOperation::CompleteActivityTask)?;
        // Before `into_inner()`, which drops the metadata the tenant lives in.
        // An operation id is all it takes to name an operation, so without the
        // caller's organization anyone could answer another tenant's
        // invocation with a result of their choosing.
        let caller = crate::grpc::tenant::organization_from_metadata(request.metadata());
        let req = request.into_inner();

        debug!(
            "CompleteActorOperation: service_id={}, operation_id={}, status={:?}",
            req.service_id,
            req.operation_id,
            ExecutionStatus::try_from(req.status).unwrap_or(ExecutionStatus::Unspecified)
        );

        let result = crate::actor::router::OperationResult {
            result: req.result,
            status: ExecutionStatus::try_from(req.status).unwrap_or(ExecutionStatus::Error),
            error_message: req.error_message,
            error_code: req.error_code,
            duration_ms: req.duration_ms,
        };

        match self
            .router
            .complete_operation_as(caller, req.operation_id.clone(), result)
            .await
        {
            Ok(_) => {
                debug!(
                    "Operation completed: service_id={}, operation_id={}",
                    req.service_id, req.operation_id
                );

                Ok(Response::new(CompleteActorOperationResponse {
                    success: true,
                    error_message: String::new(),
                }))
            }
            Err(e) => {
                error!(
                    "Failed to complete operation: service_id={}, operation_id={}, error={}",
                    req.service_id, req.operation_id, e
                );

                Ok(Response::new(CompleteActorOperationResponse {
                    success: false,
                    error_message: format!("Failed to complete operation: {}", e),
                }))
            }
        }
    }

    /// Get actor state (service → server)
    ///
    /// Services call this during handler execution to retrieve state.
    async fn get_state(
        &self,
        request: Request<GetStateRequest>,
    ) -> Result<Response<GetStateResponse>, Status> {
        // Before `into_inner()`, which drops the metadata the tenant lives in.
        let caller = crate::grpc::tenant::organization_from_metadata(request.metadata());
        let req = request.into_inner();

        debug!(
            "GetState: actor={}, key={}, state_key={}, execution_id={}",
            req.actor_name, req.key, req.state_key, req.execution_id
        );

        // The storage builds the row key from the organisation in scope, and
        // nothing scoped one for these calls, so every caller's key was the
        // unscoped one and two organisations shared one row. See
        // `ActorState::build_key` for what that key is and why.
        match crate::grpc::tenant::with_organization(
            caller,
            self.storage
                .get_state(&req.actor_name, &req.key, &req.state_key),
        )
        .await
        {
            Ok(Some(state)) => {
                debug!(
                    "State found: actor={}, key={}, state_key={}, size={} bytes",
                    req.actor_name,
                    req.key,
                    req.state_key,
                    state.value.len()
                );

                Ok(Response::new(GetStateResponse {
                    value: state.value,
                    exists: true,
                    version: state.version.to_string(),
                }))
            }
            Ok(None) => {
                debug!(
                    "State not found: actor={}, key={}, state_key={}",
                    req.actor_name, req.key, req.state_key
                );

                Ok(Response::new(GetStateResponse {
                    value: Vec::new(),
                    exists: false,
                    version: String::new(),
                }))
            }
            Err(e) => {
                error!(
                    "Failed to get state: actor={}, key={}, state_key={}, error={}",
                    req.actor_name, req.key, req.state_key, e
                );

                Err(Status::internal(format!("Failed to get state: {}", e)))
            }
        }
    }

    /// Set actor state (service → server)
    ///
    /// Services call this during handler execution to persist state.
    async fn set_state(
        &self,
        request: Request<SetStateRequest>,
    ) -> Result<Response<SetStateResponse>, Status> {
        // The shared window being checked is the caller's own actor instance.
        let caller = crate::grpc::tenant::organization_from_metadata(request.metadata());
        let req = request.into_inner();

        debug!(
            "SetState: actor={}, key={}, state_key={}, size={} bytes, execution_id={}",
            req.actor_name,
            req.key,
            req.state_key,
            req.value.len(),
            req.execution_id
        );

        // Reject writes while the actor key is in a shared (read-only)
        // window: a write racing concurrent shared operations would break
        // their isolation. Writes belong in exclusive operations.
        if self
            .router
            .state_writes_blocked_for(caller, &req.actor_name, &req.key)
        {
            warn!(
                "SetState rejected during shared window: actor={}, key={}, state_key={}",
                req.actor_name, req.key, req.state_key
            );
            return Err(Status::failed_precondition(
                "state writes are not permitted while shared (read-only) operations are in flight for this actor; perform writes from an exclusive operation",
            ));
        }

        // If expected_version is provided and non-zero, use optimistic locking.
        // Version "0" or empty string means "blind write" (no version check).
        let expected = req
            .expected_version
            .parse::<i32>()
            .ok()
            .filter(|expected| *expected > 0);
        // Scoped to the caller's organisation, as `get_state` is.
        let write = async {
            match expected {
                Some(expected) => {
                    self.storage
                        .set_state_with_version(
                            &req.actor_name,
                            &req.key,
                            &req.state_key,
                            &req.value,
                            expected,
                        )
                        .await
                }
                None => {
                    self.storage
                        .set_state(&req.actor_name, &req.key, &req.state_key, &req.value)
                        .await
                }
            }
        };
        let result = crate::grpc::tenant::with_organization(caller, write).await;

        match result {
            Ok(state) => {
                info!(
                    "State set: actor={}, key={}, state_key={}, version={}",
                    req.actor_name, req.key, req.state_key, state.version
                );

                Ok(Response::new(SetStateResponse {
                    success: true,
                    version: state.version.to_string(),
                    error_message: String::new(),
                }))
            }
            Err(e) if e.to_string().contains("Version mismatch") => {
                warn!(
                    "Version conflict: actor={}, key={}, state_key={}, expected={}",
                    req.actor_name, req.key, req.state_key, req.expected_version
                );
                Ok(Response::new(SetStateResponse {
                    success: false,
                    version: String::new(),
                    error_message: format!("version_conflict: {}", e),
                }))
            }
            Err(e) => {
                error!(
                    "Failed to set state: actor={}, key={}, state_key={}, error={}",
                    req.actor_name, req.key, req.state_key, e
                );

                Ok(Response::new(SetStateResponse {
                    success: false,
                    version: String::new(),
                    error_message: format!("Failed to set state: {}", e),
                }))
            }
        }
    }

    /// Delete actor state (service → server)
    async fn delete_state(
        &self,
        request: Request<DeleteStateRequest>,
    ) -> Result<Response<DeleteStateResponse>, Status> {
        // The shared window being checked is the caller's own actor instance.
        let caller = crate::grpc::tenant::organization_from_metadata(request.metadata());
        let req = request.into_inner();

        debug!(
            "DeleteState: actor={}, key={}, state_key={}, execution_id={}",
            req.actor_name, req.key, req.state_key, req.execution_id
        );

        // Deletions are writes: reject during a shared (read-only) window.
        if self
            .router
            .state_writes_blocked_for(caller, &req.actor_name, &req.key)
        {
            warn!(
                "DeleteState rejected during shared window: actor={}, key={}, state_key={}",
                req.actor_name, req.key, req.state_key
            );
            return Err(Status::failed_precondition(
                "state writes are not permitted while shared (read-only) operations are in flight for this actor; perform writes from an exclusive operation",
            ));
        }

        // Scoped to the caller's organisation, as `get_state` is.
        match crate::grpc::tenant::with_organization(
            caller,
            self.storage
                .delete_state(&req.actor_name, &req.key, &req.state_key),
        )
        .await
        {
            Ok(existed) => {
                info!(
                    "State deleted: actor={}, key={}, state_key={}, existed={}",
                    req.actor_name, req.key, req.state_key, existed
                );

                Ok(Response::new(DeleteStateResponse {
                    success: true,
                    existed,
                }))
            }
            Err(e) => {
                error!(
                    "Failed to delete state: actor={}, key={}, state_key={}, error={}",
                    req.actor_name, req.key, req.state_key, e
                );

                Err(Status::internal(format!("Failed to delete state: {}", e)))
            }
        }
    }

    /// List state keys for an actor instance (service → server)
    async fn list_state_keys(
        &self,
        request: Request<ListStateKeysRequest>,
    ) -> Result<Response<ListStateKeysResponse>, Status> {
        // From the metadata, as the other state RPCs read it; the auth layer
        // writes the authenticated organization there.
        let org_id = crate::grpc::tenant::organization_from_metadata(request.metadata());
        let req = request.into_inner();

        debug!(
            "ListStateKeys: actor={}, key={}, prefix={:?}, execution_id={}",
            req.actor_name, req.key, req.prefix, req.execution_id
        );

        let prefix = if req.prefix.is_empty() {
            None
        } else {
            Some(req.prefix.as_str())
        };

        match self
            .storage
            .list_keys(org_id, &req.actor_name, &req.key, prefix)
            .await
        {
            Ok(keys) => {
                // Answered as the state keys the caller uses, not the rows'
                // storage keys: SDKs pass a listed key straight back to
                // GetState and DeleteState, which build the storage key from
                // it again, so a full key addressed a row that does not exist
                // and clearing an actor's state silently deleted nothing.
                let row_prefix = orcher_storage::schema::types::actor::ActorState::key_prefix(
                    org_id,
                    &req.actor_name,
                    &req.key,
                );
                let keys: Vec<String> = keys
                    .into_iter()
                    .filter_map(|key| key.strip_prefix(&row_prefix).map(str::to_string))
                    .collect();
                debug!(
                    "Listed {} state keys: actor={}, key={}",
                    keys.len(),
                    req.actor_name,
                    req.key
                );

                Ok(Response::new(ListStateKeysResponse { keys }))
            }
            Err(e) => {
                error!(
                    "Failed to list state keys: actor={}, key={}, error={}",
                    req.actor_name, req.key, e
                );

                Err(Status::internal(format!(
                    "Failed to list state keys: {}",
                    e
                )))
            }
        }
    }

    /// Invoke an actor operation (client → server)
    ///
    /// External clients call this to invoke actor operations.
    /// The server queues the operation and waits for a service to poll and execute it.
    async fn invoke_operation(
        &self,
        request: Request<InvokeOperationRequest>,
    ) -> Result<Response<InvokeOperationResponse>, Status> {
        // The invoking organization owns the operation: only its workers are
        // handed it, and only they may answer it.
        let caller = crate::grpc::tenant::organization_from_metadata(request.metadata());
        let req = request.into_inner();

        let execution_id = Uuid::new_v4().to_string();
        let start_time = std::time::Instant::now();

        info!(
            "InvokeOperation: actor={}, key={}, operation={}, execution_id={}",
            req.actor_name, req.key, req.operation, execution_id
        );

        // Fast-fail if no worker has registered a handler for this operation.
        // Otherwise the operation queues and — with capability-aware dispatch,
        // no worker will ever poll it — is only failed by the router's
        // completion timeout minutes later, even though we can tell right now
        // that nothing will execute it.
        //
        // A lingering registration from a since-dead worker still counts as
        // registered (registrations are reaped separately), so this catches
        // the common "operation never deployed / misspelled" case, not the
        // dead-worker case — that one still relies on the completion timeout
        // until the stale-service reaper is wired in.
        if !self
            .registry
            .has_handler(&req.actor_name, &req.operation)
            .await
        {
            warn!(
                "InvokeOperation rejected: no registered handler for {}.{}",
                req.actor_name, req.operation
            );
            let duration_ms = start_time.elapsed().as_millis() as u64;
            return Ok(Response::new(InvokeOperationResponse {
                result: Vec::new(),
                status: ExecutionStatus::Error as i32,
                error_message: format!(
                    "no worker is registered for operation {}.{}",
                    req.actor_name, req.operation
                ),
                error_code: "NO_HANDLER".to_string(),
                execution_id,
                duration_ms,
            }));
        }

        // The operation's mode comes from worker handler registrations (the
        // operation's author declares read-only operations as shared) —
        // callers cannot request a mode. Unknown operations and mixed fleets
        // resolve to exclusive, the safe mode.
        let mode = self
            .registry
            .resolve_operation_mode(&req.actor_name, &req.operation)
            .await;

        // Invoke operation (queues it for polling)
        match self
            .router
            .invoke_operation_for(
                caller,
                req.actor_name.clone(),
                req.key.clone(),
                req.operation.clone(),
                req.payload,
                mode,
                execution_id.clone(),
                req.metadata,
            )
            .await
        {
            Ok((operation_id, result_rx)) => {
                debug!(
                    "Operation queued: actor={}, key={}, operation={}, operation_id={}",
                    req.actor_name, req.key, req.operation, operation_id
                );

                // Wait for result from service
                match result_rx.await {
                    Ok(Ok(result)) => {
                        let duration_ms = start_time.elapsed().as_millis() as u64;
                        info!(
                            "Operation completed successfully: actor={}, key={}, operation={}, execution_id={}, duration_ms={}",
                            req.actor_name, req.key, req.operation, execution_id, duration_ms
                        );

                        Ok(Response::new(InvokeOperationResponse {
                            result,
                            status: ExecutionStatus::Success as i32,
                            error_message: String::new(),
                            error_code: String::new(),
                            execution_id,
                            duration_ms,
                        }))
                    }
                    Ok(Err(e)) => {
                        let duration_ms = start_time.elapsed().as_millis() as u64;
                        error!(
                            "Operation failed: actor={}, key={}, operation={}, execution_id={}, error={}",
                            req.actor_name, req.key, req.operation, execution_id, e
                        );

                        Ok(Response::new(InvokeOperationResponse {
                            result: Vec::new(),
                            status: ExecutionStatus::Error as i32,
                            error_message: e.to_string(),
                            error_code: "EXECUTION_ERROR".to_string(),
                            execution_id,
                            duration_ms,
                        }))
                    }
                    Err(_) => {
                        let duration_ms = start_time.elapsed().as_millis() as u64;
                        error!(
                            "Operation result channel closed: actor={}, key={}, operation={}, execution_id={}",
                            req.actor_name, req.key, req.operation, execution_id
                        );

                        Ok(Response::new(InvokeOperationResponse {
                            result: Vec::new(),
                            status: ExecutionStatus::Error as i32,
                            error_message: "Operation result channel closed".to_string(),
                            error_code: "CHANNEL_CLOSED".to_string(),
                            execution_id,
                            duration_ms,
                        }))
                    }
                }
            }
            Err(e) => {
                let duration_ms = start_time.elapsed().as_millis() as u64;
                error!(
                    "Failed to queue operation: actor={}, key={}, operation={}, execution_id={}, error={}",
                    req.actor_name, req.key, req.operation, execution_id, e
                );

                Ok(Response::new(InvokeOperationResponse {
                    result: Vec::new(),
                    status: ExecutionStatus::Error as i32,
                    error_message: e.to_string(),
                    error_code: "QUEUE_ERROR".to_string(),
                    execution_id,
                    duration_ms,
                }))
            }
        }
    }

    /// Register actor handlers (service → server)
    ///
    /// Services call this on startup to register their available handlers.
    async fn register_handlers(
        &self,
        request: Request<RegisterHandlersRequest>,
    ) -> Result<Response<RegisterHandlersResponse>, Status> {
        let req = request.into_inner();

        info!(
            "RegisterHandlers: service_id={}, handlers={}",
            req.service_id,
            req.handlers.len()
        );

        // Convert proto handlers to internal format
        let handlers: Vec<HandlerInfo> = req
            .handlers
            .into_iter()
            .map(|h| HandlerInfo {
                actor_name: h.actor_name,
                operation: h.operation,
                mode: OperationMode::try_from(h.mode).unwrap_or(OperationMode::Exclusive),
                metadata: h.metadata,
            })
            .collect();

        let service_info = ServiceInfo {
            service_id: req.service_id.clone(),
            endpoint: String::new(), // Not needed in polling model
            handlers,
            status: WorkerStatus::Healthy as i32,
            registered_at: Timestamptz::now(),
            last_heartbeat: Timestamptz::now(),
            metadata: req.metadata,
            metrics: None,
        };

        match self.registry.register(service_info).await {
            Ok(registration_id) => {
                info!(
                    "Service registered: service_id={}, registration_id={}",
                    req.service_id, registration_id
                );

                Ok(Response::new(RegisterHandlersResponse {
                    success: true,
                    registration_id,
                    handlers_registered: self
                        .registry
                        .get_service(&req.service_id)
                        .await
                        .map(|s| s.handlers.len() as u32)
                        .unwrap_or(0),
                    error_message: String::new(),
                }))
            }
            Err(e) => {
                error!(
                    "Failed to register service: service_id={}, error={}",
                    req.service_id, e
                );

                Ok(Response::new(RegisterHandlersResponse {
                    success: false,
                    registration_id: String::new(),
                    handlers_registered: 0,
                    error_message: format!("Failed to register service: {}", e),
                }))
            }
        }
    }

    /// Service heartbeat (service → server)
    ///
    /// Services send periodic heartbeats to indicate they are alive and healthy.
    async fn heartbeat(
        &self,
        request: Request<HeartbeatRequest>,
    ) -> Result<Response<HeartbeatResponse>, Status> {
        let req = request.into_inner();

        debug!(
            "Heartbeat: service_id={}, registration_id={}, status={:?}",
            req.service_id, req.registration_id, req.status
        );

        let status = req.status;

        match self
            .registry
            .heartbeat(&req.service_id, status, req.metrics)
            .await
        {
            Ok(true) => {
                debug!("Heartbeat updated: service_id={}", req.service_id);

                Ok(Response::new(HeartbeatResponse {
                    success: true,
                    re_register: false,
                    timestamp: Timestamptz::now().timestamp(),
                }))
            }
            Ok(false) => {
                warn!(
                    "Heartbeat from unregistered service: service_id={}",
                    req.service_id
                );

                Ok(Response::new(HeartbeatResponse {
                    success: false,
                    re_register: true,
                    timestamp: Timestamptz::now().timestamp(),
                }))
            }
            Err(e) => {
                error!(
                    "Failed to process heartbeat: service_id={}, error={}",
                    req.service_id, e
                );

                Err(Status::internal(format!(
                    "Failed to process heartbeat: {}",
                    e
                )))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_actor_service_impl_creation() {
        // Note: This requires actual Redis and PostgreSQL connections
        // Use integration tests with testcontainers for real testing
        assert!(true);
    }
}

/// An actor operation belongs to the organization that invoked it.
///
/// Operations wait in one queue shared by every tenant and are answered by
/// operation id alone, so a worker of any organization was handed any
/// organization's operation, and any caller that learned an operation id could
/// complete it with a result of its choosing.
#[cfg(test)]
mod tenancy {
    use super::*;
    use orcher_proto::orcher::v1::ActorHandler;
    use std::time::Duration;

    const ACTOR: &str = "Ledger";
    const OPERATION: &str = "post";

    fn service() -> ActorServiceImpl {
        // The state storage is never touched here; a lazy pool never connects.
        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgresql://localhost/unused")
            .expect("a lazy pool");
        ActorServiceImpl::new(
            Arc::new(ActorRouter::new().with_completion_timeout(Duration::from_secs(10))),
            Arc::new(ActorStateStorage::new(pool)),
            Arc::new(ServiceRegistry::new()),
        )
    }

    fn from<T>(message: T, organization: Uuid) -> Request<T> {
        let mut request = Request::new(message);
        crate::grpc::tenant::set_organization(&mut request, &organization);
        request
    }

    async fn register(service: &ActorServiceImpl, service_id: &str, organization: Uuid) {
        let registered = service
            .register_handlers(from(
                RegisterHandlersRequest {
                    service_id: service_id.to_string(),
                    handlers: vec![ActorHandler {
                        actor_name: ACTOR.to_string(),
                        operation: OPERATION.to_string(),
                        ..Default::default()
                    }],
                    ..Default::default()
                },
                organization,
            ))
            .await
            .expect("register handlers")
            .into_inner();
        assert!(registered.success);
    }

    async fn poll(
        service: &ActorServiceImpl,
        service_id: &str,
        organization: Uuid,
        timeout_ms: u64,
    ) -> Vec<orcher_proto::orcher::v1::ActorOperation> {
        service
            .poll_actor_operation(from(
                PollActorOperationRequest {
                    service_id: service_id.to_string(),
                    max_operations: 10,
                    timeout_ms,
                },
                organization,
            ))
            .await
            .expect("poll")
            .into_inner()
            .operations
    }

    async fn complete(
        service: &ActorServiceImpl,
        operation_id: &str,
        organization: Uuid,
        result: &[u8],
    ) -> CompleteActorOperationResponse {
        service
            .complete_actor_operation(from(
                CompleteActorOperationRequest {
                    service_id: "worker".to_string(),
                    operation_id: operation_id.to_string(),
                    result: result.to_vec(),
                    status: ExecutionStatus::Success as i32,
                    ..Default::default()
                },
                organization,
            ))
            .await
            .expect("complete")
            .into_inner()
    }

    #[tokio::test]
    async fn another_organization_can_neither_take_nor_answer_an_operation() {
        let service = service();
        let owner = Uuid::new_v4();
        let intruder = Uuid::new_v4();
        register(&service, "owner-worker", owner).await;
        register(&service, "intruder-worker", intruder).await;

        let invoker = service.clone();
        let invocation = tokio::spawn(async move {
            invoker
                .invoke_operation(from(
                    InvokeOperationRequest {
                        actor_name: ACTOR.to_string(),
                        key: "account-1".to_string(),
                        operation: OPERATION.to_string(),
                        payload: b"credit 10".to_vec(),
                        ..Default::default()
                    },
                    owner,
                ))
                .await
                .expect("invoke")
                .into_inner()
        });

        // Wait until the operation is queued, then check it is not handed to
        // the other organization's worker.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while service.router().pending_results_count() == 0 {
            assert!(tokio::time::Instant::now() < deadline, "never queued");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(
            poll(&service, "intruder-worker", intruder, 200)
                .await
                .is_empty(),
            "another organization's worker is handed nothing"
        );

        let taken = poll(&service, "owner-worker", owner, 2_000).await;
        assert_eq!(taken.len(), 1, "the owner's worker is handed it");
        let operation_id = taken[0].operation_id.clone();

        let refused = complete(&service, &operation_id, intruder, b"forged").await;
        assert!(
            !refused.success,
            "another organization cannot answer the operation"
        );

        let answered = complete(&service, &operation_id, owner, b"posted").await;
        assert!(answered.success, "{}", answered.error_message);

        let response = invocation.await.expect("invocation task");
        assert_eq!(
            response.result, b"posted",
            "the invoker receives its own worker's answer, not the forged one"
        );
    }

    /// The same actor name and key in two organizations are two instances:
    /// one organization's exclusive operation, waiting for a worker of its
    /// own, does not hold up the other's.
    #[tokio::test]
    async fn one_organizations_waiting_operation_does_not_block_another() {
        let service = service();
        let absent = Uuid::new_v4();
        let present = Uuid::new_v4();
        // Only `present` has a worker; `absent`'s operation can only wait.
        register(&service, "present-worker", present).await;

        let invoke = |organization: Uuid, payload: &'static [u8]| {
            let invoker = service.clone();
            tokio::spawn(async move {
                invoker
                    .invoke_operation(from(
                        InvokeOperationRequest {
                            actor_name: ACTOR.to_string(),
                            key: "account-1".to_string(),
                            operation: OPERATION.to_string(),
                            payload: payload.to_vec(),
                            ..Default::default()
                        },
                        organization,
                    ))
                    .await
                    .expect("invoke")
                    .into_inner()
            })
        };
        let waiting = invoke(absent, b"absent");
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while service.router().pending_results_count() == 0 {
            assert!(tokio::time::Instant::now() < deadline, "never dispatched");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let answered = invoke(present, b"present");

        let taken = poll(&service, "present-worker", present, 2_000).await;
        assert_eq!(
            taken.len(),
            1,
            "the other organization's operation is dispatched at once"
        );
        assert_eq!(taken[0].payload, b"present");
        let done = complete(&service, &taken[0].operation_id, present, b"ok").await;
        assert!(done.success, "{}", done.error_message);
        assert_eq!(answered.await.expect("invocation task").result, b"ok");

        waiting.abort();
    }
}
