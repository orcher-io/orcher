//! Shared application state for Axum request handlers.

use crate::config::OrchestratorConfig;
use crate::grpc::clients::namespace::NamespaceServiceClient;
use crate::grpc::clients::workflow::WorkflowServiceClient;
use orcher_common::error::OrcherResult;
use orcher_storage::repositories::domains::schedule::ScheduleRepository;
use orcher_storage::repositories::domains::workflow_execution::logs::WorkflowExecutionLogsRepository;
use orcher_storage::repositories::domains::workflow_execution::repository::WorkflowExecutionRepository;
use sqlx::PgPool;
use std::sync::Arc;
use std::time::Instant;
use tracing::{info, warn};

use crate::services::{RegistryService, ScheduleService};

/// Core application state — orchestration services only.
///
/// Cloud features (auth, users, orgs, sessions, audit) are provided by the
/// `orcher-account` crate in the private cloud repo. The cloud gateway
/// wraps this state and adds account services.
pub struct AppState {
    pool: PgPool,
    auth_provider: Arc<dyn crate::auth::AuthProvider>,
    /// Who callers are, when set: the engine's one authentication seam. Unset,
    /// the REST API identifies callers with `auth_provider`.
    authenticator: Option<Arc<dyn orcher_core::auth::Authenticator>>,
    /// The deployment's declared tenancy, which `authenticator`'s callers are
    /// resolved for.
    tenancy: Option<orcher_core::auth::Tenancy>,
    security_policy: Arc<dyn crate::security::SecurityPolicy>,
    /// Shared so release-management endpoints reach the same instance the rest
    /// of the server uses — a freshly constructed repository has no shard
    /// resolver, so its pool scan would miss rows on secondary shard databases.
    execution_repository: Arc<WorkflowExecutionRepository>,
    registry_service: Arc<RegistryService>,
    workflow_service: Arc<crate::services::workflow::WorkflowService>,
    schedule_service: Arc<ScheduleService>,
    workflow_client: Option<Arc<WorkflowServiceClient>>,
    namespace_client: Option<Arc<NamespaceServiceClient>>,
    orchestrator_config: Arc<OrchestratorConfig>,
    started_at: Instant,
}

impl AppState {
    /// Build the state with the orchestrator configuration read from the
    /// environment (`ORCHESTRATOR_ENDPOINT` and friends).
    pub async fn new(pool: PgPool) -> OrcherResult<Self> {
        let orchestrator_config = OrchestratorConfig::from_env().unwrap_or_else(|e| {
            warn!("Failed to load orchestrator config: {}, using defaults", e);
            OrchestratorConfig::development()
        });
        Self::with_orchestrator_config(pool, orchestrator_config).await
    }

    /// Build the state against the orchestrator `orchestrator_config` names,
    /// which a caller has resolved from its own flags and the environment.
    pub async fn with_orchestrator_config(
        pool: PgPool,
        orchestrator_config: OrchestratorConfig,
    ) -> OrcherResult<Self> {
        let execution_repository = Arc::new(WorkflowExecutionRepository::new(pool.clone()));
        let logs_repository = Arc::new(WorkflowExecutionLogsRepository::new(pool.clone()));

        // Connect to orchestrator via gRPC
        let orchestrator_config = Arc::new(orchestrator_config);

        let (workflow_client, namespace_client, workflow_service) = if let Err(e) =
            orchestrator_config.validate()
        {
            warn!("Invalid orchestrator config: {}, using stub mode", e);
            (
                None,
                None,
                Arc::new(crate::services::workflow::WorkflowService::new_stub(
                    execution_repository.clone(),
                    logs_repository.clone(),
                )),
            )
        } else {
            match WorkflowServiceClient::connect(orchestrator_config.endpoint.clone()).await {
                Ok(client) => {
                    let client = Arc::new(client);
                    info!(
                        "Connected to orchestrator at {}",
                        orchestrator_config.endpoint
                    );

                    let workflow_service =
                        Arc::new(crate::services::workflow::WorkflowService::new(
                            execution_repository.clone(),
                            logs_repository.clone(),
                            client.clone(),
                        ));

                    let ns_client =
                        match NamespaceServiceClient::connect(orchestrator_config.endpoint.clone())
                            .await
                        {
                            Ok(c) => {
                                info!("Connected NamespaceServiceClient");
                                Some(Arc::new(c))
                            }
                            Err(e) => {
                                warn!("Failed to connect NamespaceServiceClient: {}", e);
                                None
                            }
                        };

                    (Some(client), ns_client, workflow_service)
                }
                Err(e) => {
                    warn!("Failed to connect to orchestrator: {}, using stub mode", e);
                    (
                        None,
                        None,
                        Arc::new(crate::services::workflow::WorkflowService::new_stub(
                            execution_repository.clone(),
                            logs_repository.clone(),
                        )),
                    )
                }
            }
        };

        Ok(Self::from_parts(
            pool,
            orchestrator_config,
            execution_repository,
            workflow_service,
            workflow_client,
            namespace_client,
        ))
    }

    /// Build the state for the engine serving its own REST API: the calls a
    /// handler makes go over `channel`, the engine's in-process loopback, and
    /// are authenticated there with the caller's own credential.
    pub fn with_orchestrator_channel(
        pool: PgPool,
        orchestrator_config: OrchestratorConfig,
        channel: tonic::transport::Channel,
    ) -> Self {
        let execution_repository = Arc::new(WorkflowExecutionRepository::new(pool.clone()));
        let logs_repository = Arc::new(WorkflowExecutionLogsRepository::new(pool.clone()));
        let endpoint = orchestrator_config.endpoint.clone();
        let workflow_client = Arc::new(WorkflowServiceClient::from_channel(
            channel.clone(),
            endpoint.clone(),
        ));
        let namespace_client = Arc::new(NamespaceServiceClient::from_channel(channel, endpoint));
        let workflow_service = Arc::new(crate::services::workflow::WorkflowService::new(
            execution_repository.clone(),
            logs_repository,
            workflow_client.clone(),
        ));
        Self::from_parts(
            pool,
            Arc::new(orchestrator_config),
            execution_repository,
            workflow_service,
            Some(workflow_client),
            Some(namespace_client),
        )
    }

    fn from_parts(
        pool: PgPool,
        orchestrator_config: Arc<OrchestratorConfig>,
        execution_repository: Arc<WorkflowExecutionRepository>,
        workflow_service: Arc<crate::services::workflow::WorkflowService>,
        workflow_client: Option<Arc<WorkflowServiceClient>>,
        namespace_client: Option<Arc<NamespaceServiceClient>>,
    ) -> Self {
        let schedule_repository = Arc::new(ScheduleRepository::new(pool.clone()));
        let registry_service = Arc::new(RegistryService::new());
        let schedule_service = Arc::new(ScheduleService::new(schedule_repository));
        let auth_provider: Arc<dyn crate::auth::AuthProvider> =
            Arc::new(crate::auth::NoOpAuthProvider);
        let security_policy: Arc<dyn crate::security::SecurityPolicy> =
            Arc::new(crate::security::DefaultSecurityPolicy::default());

        Self {
            pool,
            auth_provider,
            authenticator: None,
            tenancy: None,
            security_policy,
            execution_repository: execution_repository.clone(),
            registry_service,
            workflow_service,
            schedule_service,
            workflow_client,
            namespace_client,
            orchestrator_config,
            started_at: Instant::now(),
        }
    }

    #[must_use]
    /// The workflow execution repository, wired with whatever shard resolver
    /// this deployment configured.
    pub fn execution_repository(&self) -> &Arc<WorkflowExecutionRepository> {
        &self.execution_repository
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    #[must_use]
    pub fn auth_provider(&self) -> &dyn crate::auth::AuthProvider {
        self.auth_provider.as_ref()
    }

    pub fn set_auth_provider(&mut self, provider: Arc<dyn crate::auth::AuthProvider>) {
        self.auth_provider = provider;
    }

    /// The authenticator REST requests are identified with, if one is set.
    #[must_use]
    pub fn authenticator(&self) -> Option<&Arc<dyn orcher_core::auth::Authenticator>> {
        self.authenticator.as_ref()
    }

    /// Identify REST callers with `authenticator`, the one the gRPC API uses,
    /// rather than with the [`AuthProvider`](crate::auth::AuthProvider).
    pub fn set_authenticator(&mut self, authenticator: Arc<dyn orcher_core::auth::Authenticator>) {
        self.authenticator = Some(authenticator);
    }

    /// The deployment's declared tenancy, if declared.
    #[must_use]
    pub fn tenancy(&self) -> Option<orcher_core::auth::Tenancy> {
        self.tenancy
    }

    /// Resolve REST callers' organisations for a deployment of `tenancy`.
    pub fn set_tenancy(&mut self, tenancy: Option<orcher_core::auth::Tenancy>) {
        self.tenancy = tenancy;
    }

    /// The active security policy (OSS default, or an injected implementation).
    #[must_use]
    pub fn security_policy(&self) -> &dyn crate::security::SecurityPolicy {
        self.security_policy.as_ref()
    }

    /// Replace the security policy. Used by the cloud gateway — or any
    /// self-hosted deployment — to enforce its own policy in place of
    /// [`crate::security::DefaultSecurityPolicy`].
    pub fn set_security_policy(&mut self, policy: Arc<dyn crate::security::SecurityPolicy>) {
        self.security_policy = policy;
    }

    #[must_use]
    pub fn registry_service(&self) -> &RegistryService {
        &self.registry_service
    }

    #[must_use]
    pub fn workflow_service(&self) -> &Arc<crate::services::workflow::WorkflowService> {
        &self.workflow_service
    }

    #[must_use]
    pub fn schedule_service(&self) -> &Arc<ScheduleService> {
        &self.schedule_service
    }

    #[must_use]
    pub fn workflow_client(&self) -> Option<&Arc<WorkflowServiceClient>> {
        self.workflow_client.as_ref()
    }

    #[must_use]
    pub fn namespace_client(&self) -> Option<&Arc<NamespaceServiceClient>> {
        self.namespace_client.as_ref()
    }

    /// Non-sensitive orchestrator configuration (endpoint, namespace, timeouts).
    #[must_use]
    pub fn orchestrator_config(&self) -> &OrchestratorConfig {
        &self.orchestrator_config
    }

    /// Whether a gRPC connection to the orchestrator was established at startup.
    #[must_use]
    pub fn orchestrator_connected(&self) -> bool {
        self.workflow_client.is_some()
    }

    /// Seconds elapsed since this gateway state was constructed (uptime).
    #[must_use]
    pub fn uptime_secs(&self) -> u64 {
        self.started_at.elapsed().as_secs()
    }

    /// Create a minimal AppState for unit tests (no gRPC).
    pub fn new_for_test(pool: PgPool) -> Self {
        use orcher_storage::repositories::domains::workflow_execution::{
            logs::WorkflowExecutionLogsRepository, repository::WorkflowExecutionRepository,
        };

        let schedule_repository = Arc::new(ScheduleRepository::new(pool.clone()));
        let registry_service = Arc::new(RegistryService::new());
        let schedule_service = Arc::new(ScheduleService::new(schedule_repository));
        let execution_repository = Arc::new(WorkflowExecutionRepository::new(pool.clone()));
        let logs_repository = Arc::new(WorkflowExecutionLogsRepository::new(pool.clone()));
        let workflow_service = Arc::new(crate::services::workflow::WorkflowService::new_stub(
            execution_repository.clone(),
            logs_repository,
        ));
        let auth_provider: Arc<dyn crate::auth::AuthProvider> =
            Arc::new(crate::auth::NoOpAuthProvider);
        let security_policy: Arc<dyn crate::security::SecurityPolicy> =
            Arc::new(crate::security::DefaultSecurityPolicy::default());

        Self {
            pool,
            auth_provider,
            authenticator: None,
            tenancy: None,
            security_policy,
            execution_repository: execution_repository.clone(),
            registry_service,
            workflow_service,
            schedule_service,
            workflow_client: None,
            namespace_client: None,
            orchestrator_config: Arc::new(OrchestratorConfig::development()),
            started_at: Instant::now(),
        }
    }

    pub async fn shutdown(&self) -> OrcherResult<()> {
        info!("Shutting down application state");
        Ok(())
    }
}

impl Clone for AppState {
    fn clone(&self) -> Self {
        Self {
            pool: self.pool.clone(),
            auth_provider: self.auth_provider.clone(),
            authenticator: self.authenticator.clone(),
            tenancy: self.tenancy,
            security_policy: self.security_policy.clone(),
            execution_repository: self.execution_repository.clone(),
            registry_service: self.registry_service.clone(),
            workflow_service: self.workflow_service.clone(),
            schedule_service: self.schedule_service.clone(),
            workflow_client: self.workflow_client.clone(),
            namespace_client: self.namespace_client.clone(),
            orchestrator_config: self.orchestrator_config.clone(),
            started_at: self.started_at,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The state talks to the orchestrator its caller named, not one it reads
    /// from the environment behind the caller's back.
    #[tokio::test]
    async fn the_state_keeps_the_orchestrator_it_was_given() {
        let pool = PgPool::connect_lazy("postgresql://localhost/test").unwrap();
        let config = OrchestratorConfig {
            endpoint: "http://127.0.0.1:1".to_string(),
            ..OrchestratorConfig::development()
        };
        let state = AppState::with_orchestrator_config(pool, config)
            .await
            .unwrap();
        assert_eq!(state.orchestrator_config().endpoint, "http://127.0.0.1:1");
        assert!(!state.orchestrator_connected());
    }
}
