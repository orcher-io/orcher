//! gRPC and HTTP server managers with graceful shutdown

use crate::config::{ServerConfig, TlsServerConfig};
use crate::health::HealthManager;
use anyhow::{Context, Result};

use orcher_core::grpc::auth_layer::AuthLayer;
use orcher_core::grpc::services::execution::ExecutionService;
use orcher_core::grpc::services::namespace::NamespaceServiceImpl;
use orcher_core::grpc::services::query::QueryService;
use orcher_core::grpc::services::workflow::WorkflowService;
use orcher_core::grpc::services::ActorServiceImpl;
use orcher_core::grpc::services::WorkerServiceImpl;
use std::net::SocketAddr;
use tokio::sync::broadcast;
use tokio::task::JoinHandle;
use tonic::transport::Server;
use tracing::{info, warn};

/// Manages the gRPC server lifecycle with modern pull-based services
pub struct GrpcServerManager {
    config: ServerConfig,
    /// The largest message any service sends or accepts.
    max_message_bytes: usize,
    auth_layer: AuthLayer,
    tls_config: Option<TlsServerConfig>,
    execution_service: Option<ExecutionService>,
    workflow_service: Option<WorkflowService>,
    query_service: Option<QueryService>,
    actor_service: Option<ActorServiceImpl>,
    worker_service: Option<WorkerServiceImpl>,
    namespace_service: Option<NamespaceServiceImpl>,
    /// In-process connections to serve beside the socket's, for the REST API.
    loopback: Option<orcher_core::grpc::loopback::LoopbackIncoming>,
}

impl GrpcServerManager {
    /// Create a new gRPC server manager with all services
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        config: ServerConfig,
        max_message_bytes: usize,
        auth_layer: AuthLayer,
        tls_config: Option<TlsServerConfig>,
        execution_service: ExecutionService,
        workflow_service: WorkflowService,
        query_service: QueryService,
        actor_service: ActorServiceImpl,
        worker_service: WorkerServiceImpl,
        namespace_service: NamespaceServiceImpl,
    ) -> Self {
        Self {
            config,
            max_message_bytes,
            auth_layer,
            tls_config,
            execution_service: Some(execution_service),
            workflow_service: Some(workflow_service),
            query_service: Some(query_service),
            actor_service: Some(actor_service),
            worker_service: Some(worker_service),
            namespace_service: Some(namespace_service),
            loopback: None,
        }
    }

    /// Also serve the connections `incoming` receives: the engine's own REST
    /// API calling its gRPC services in-process. They pass through the same
    /// authentication as a socket's, without TLS, which an in-memory
    /// connection has no use for.
    pub fn with_loopback(
        mut self,
        incoming: orcher_core::grpc::loopback::LoopbackIncoming,
    ) -> Self {
        self.loopback = Some(incoming);
        self
    }

    /// Start the gRPC server with graceful shutdown support.
    ///
    /// Everything that can fail before the first request — the address, TLS,
    /// binding the port — happens here, before this returns, so a failure is
    /// an error from `start` and the engine does not come up. It used to happen
    /// inside the spawned task, which logged and returned: the engine then
    /// marked itself ready and ran on, doing background work but reachable by
    /// no worker. The returned task resolves when the server stops; `Ok` only
    /// after the shutdown signal.
    pub async fn start(
        self,
        mut shutdown: broadcast::Receiver<()>,
    ) -> Result<JoinHandle<Result<()>>> {
        use orcher_proto::orcher::v1::actor_service_server::ActorServiceServer;

        let addr = self
            .config
            .grpc_addr
            .parse::<SocketAddr>()
            .context("Invalid gRPC address")?;

        info!("Starting gRPC server on {}", addr);

        let max_streams = self.config.max_concurrent_streams;

        // Build base server configuration with auth layer applied to all services
        let mut builder = Server::builder()
            .max_concurrent_streams(Some(max_streams))
            .http2_keepalive_interval(Some(std::time::Duration::from_secs(30)))
            .http2_keepalive_timeout(Some(std::time::Duration::from_secs(70)))
            .http2_max_pending_accept_reset_streams(Some(200));

        if let Some(ref tls_cfg) = self.tls_config {
            let tls = Self::build_tls_config(tls_cfg)
                .await
                .context("Failed to configure TLS for the gRPC server")?;
            builder = builder
                .tls_config(tls)
                .context("Failed to apply TLS config to the gRPC server")?;
            info!(
                cert_path = %tls_cfg.cert_path,
                require_client_cert = tls_cfg.require_client_cert,
                "TLS enabled for gRPC server"
            );
        }

        let loopback = self.loopback.map(|incoming| {
            let builder = Server::builder()
                .max_concurrent_streams(Some(max_streams))
                .layer(self.auth_layer.clone());
            (builder, incoming)
        });
        let mut builder = builder.layer(self.auth_layer);

        // Build routes dynamically via RoutesBuilder — avoids combinatorial
        // match explosion as more services are added.
        let mut service_names: Vec<&str> = Vec::new();
        let mut routes_builder = tonic::service::Routes::builder();

        // Every service sends and accepts messages up to the configured limit,
        // in place of tonic's 4 MiB default. A message over it is refused by
        // tonic before any handler runs; payloads are held to a smaller limit
        // by the handlers, which can say what was too large.
        let max = self.max_message_bytes;
        macro_rules! sized {
            ($server:expr) => {
                $server
                    .max_decoding_message_size(max)
                    .max_encoding_message_size(max)
            };
        }

        if let Some(exec) = self.execution_service {
            routes_builder.add_service(sized!(exec.into_server()));
            service_names.push("ExecutionService");
        }
        if let Some(wf) = self.workflow_service {
            routes_builder.add_service(sized!(wf.into_server()));
            service_names.push("WorkflowService");
        }
        if let Some(query) = self.query_service {
            routes_builder.add_service(sized!(query.into_server()));
            service_names.push("QueryService");
        }
        if let Some(actor) = self.actor_service {
            routes_builder.add_service(sized!(ActorServiceServer::new(actor)));
            service_names.push("ActorService");
        }
        if let Some(worker) = self.worker_service {
            routes_builder.add_service(sized!(worker.into_server()));
            service_names.push("WorkerService");
        }
        if let Some(ns) = self.namespace_service {
            routes_builder.add_service(sized!(ns.into_server()));
            service_names.push("NamespaceService");
        }

        if service_names.is_empty() {
            warn!("No gRPC services registered - server will start but accept no requests");
            return Ok(tokio::spawn(async move {
                let _ = shutdown.recv().await;
                info!("gRPC server received shutdown signal");
                Ok(())
            }));
        }

        info!(services = ?service_names, "Adding gRPC services");
        let routes = routes_builder.routes();
        let loopback =
            loopback.map(|(mut builder, incoming)| (builder.add_routes(routes.clone()), incoming));
        let router = builder.add_routes(routes);

        // Bound here, not in the task, so a port already in use fails startup.
        // TLS, when configured, is applied by tonic to this listener's
        // connections exactly as it would be to one it bound itself.
        let listener = tokio::net::TcpListener::bind(addr)
            .await
            .with_context(|| format!("Failed to bind gRPC server on {addr}"))?;

        info!(
            addr = %addr,
            max_concurrent_streams = self.config.max_concurrent_streams,
            max_message_bytes = self.max_message_bytes,
            tls = self.tls_config.is_some(),
            "gRPC server listening"
        );

        // Every connection writes its responses at once. With Nagle's
        // algorithm on, a response too small to fill a packet waited for the
        // worker's delayed acknowledgement of the one before, about 40 ms on
        // Linux: one such wait for every task handed out and every activation
        // carrying few journal entries. Set on each connection, since tonic's
        // own setting applies only to listeners it opens itself.
        let incoming = tokio_stream::StreamExt::map(
            tokio_stream::wrappers::TcpListenerStream::new(listener),
            |accepted| {
                accepted.inspect(|stream| {
                    if let Err(e) = stream.set_nodelay(true) {
                        warn!(error = %e, "Could not turn off Nagle's algorithm on a gRPC connection");
                    }
                })
            },
        );

        let in_process = async move {
            match loopback {
                Some((router, incoming)) => router.serve_with_incoming(incoming).await,
                None => std::future::pending().await,
            }
        };

        Ok(tokio::spawn(async move {
            tokio::select! {
                result = router.serve_with_incoming(incoming) => {
                    result.context("gRPC server failed")?;
                    anyhow::bail!("gRPC server stopped without a shutdown signal");
                }
                result = in_process => {
                    result.context("in-process gRPC server failed")?;
                    anyhow::bail!("in-process gRPC server stopped without a shutdown signal");
                }
                _ = shutdown.recv() => {
                    info!("gRPC server received shutdown signal");
                }
            }
            info!("gRPC server stopped");
            Ok(())
        }))
    }

    /// Build tonic ServerTlsConfig from our TlsServerConfig.
    async fn build_tls_config(
        cfg: &TlsServerConfig,
    ) -> anyhow::Result<tonic::transport::ServerTlsConfig> {
        use tonic::transport::{Certificate, Identity, ServerTlsConfig};

        let cert_pem = tokio::fs::read(&cfg.cert_path)
            .await
            .with_context(|| format!("Failed to read TLS cert: {}", cfg.cert_path))?;
        let key_pem = tokio::fs::read(&cfg.key_path)
            .await
            .with_context(|| format!("Failed to read TLS key: {}", cfg.key_path))?;

        let identity = Identity::from_pem(cert_pem, key_pem);
        let mut tls = ServerTlsConfig::new().identity(identity);

        // mTLS: if CA cert is provided and client certs are required, configure client verification
        if cfg.require_client_cert {
            if let Some(ref ca_path) = cfg.ca_cert_path {
                let ca_pem = tokio::fs::read(ca_path)
                    .await
                    .with_context(|| format!("Failed to read CA cert: {}", ca_path))?;
                tls = tls.client_ca_root(Certificate::from_pem(ca_pem));
                info!("mTLS enabled — client certificates required");
            }
        }

        Ok(tls)
    }
}

/// TLS for dialing a peer engine, mirroring how this engine serves.
///
/// Peers run the same configuration, so what this node presents is what they
/// present. Their certificate is verified against the configured CA and only
/// that: with no CA configured this is an error, because trusting this node's
/// own certificate instead would pin every peer to one shared leaf. With
/// client certificates required, this node presents its own certificate as
/// the client identity: the peer requires one of every caller, and in that
/// mode any holder of a valid certificate is authorised alike, so this grants
/// nothing a worker with a certificate does not have.
///
/// Peers are dialed by the address they advertise, often a pod IP that no
/// certificate names; `ORCHER_FORWARD_TLS_DOMAIN` sets the name verified
/// instead.
pub async fn build_peer_tls_config(
    cfg: &TlsServerConfig,
) -> anyhow::Result<tonic::transport::ClientTlsConfig> {
    use tonic::transport::{Certificate, ClientTlsConfig, Identity};

    let Some(ca_path) = &cfg.ca_cert_path else {
        anyhow::bail!("no CA configured (security.tls.ca_cert_path) to verify peers against");
    };
    let trust = tokio::fs::read(ca_path)
        .await
        .with_context(|| format!("Failed to read CA cert: {}", ca_path))?;
    let mut tls = ClientTlsConfig::new().ca_certificate(Certificate::from_pem(trust));
    if cfg.require_client_cert {
        let cert_pem = tokio::fs::read(&cfg.cert_path)
            .await
            .with_context(|| format!("Failed to read TLS cert: {}", cfg.cert_path))?;
        let key_pem = tokio::fs::read(&cfg.key_path)
            .await
            .with_context(|| format!("Failed to read TLS key: {}", cfg.key_path))?;
        tls = tls.identity(Identity::from_pem(cert_pem, key_pem));
    }
    if let Ok(domain) = std::env::var("ORCHER_FORWARD_TLS_DOMAIN") {
        if !domain.trim().is_empty() {
            tls = tls.domain_name(domain.trim());
        }
    }
    Ok(tls)
}

/// Manages the HTTP server lifecycle
pub struct HttpServerManager {
    config: ServerConfig,
    health: HealthManager,
    rest_api: Option<axum::Router>,
    dashboard: Option<std::path::PathBuf>,
}

impl HttpServerManager {
    /// Create a new HTTP server manager
    pub fn new(config: ServerConfig, health: HealthManager) -> Self {
        Self {
            config,
            health,
            rest_api: None,
            dashboard: None,
        }
    }

    /// Also serve the dashboard in `dir`, after every other route.
    pub fn with_dashboard(mut self, dir: std::path::PathBuf) -> Self {
        self.dashboard = Some(dir);
        self
    }

    /// Also serve `router`, the REST API, beside the health endpoints.
    pub fn with_rest_api(mut self, router: axum::Router) -> Self {
        self.rest_api = Some(router);
        self
    }

    /// Start the HTTP server with graceful shutdown support.
    ///
    /// The port is bound before this returns, for the same reason as the gRPC
    /// server: a bind failure used to panic the spawned task and leave the
    /// engine running with no health or metrics endpoint.
    pub async fn start(
        self,
        mut shutdown: broadcast::Receiver<()>,
    ) -> Result<JoinHandle<Result<()>>> {
        let addr = self
            .config
            .http_addr
            .parse::<SocketAddr>()
            .context("Invalid HTTP address")?;

        info!("Starting HTTP server on {}", addr);

        let router = match self.rest_api {
            Some(rest_api) => self.health.router().merge(rest_api),
            None => self.health.router(),
        };
        let router = match &self.dashboard {
            Some(dir) => router.fallback_service(crate::ui::service(dir)),
            None => router,
        };

        let listener = tokio::net::TcpListener::bind(addr)
            .await
            .with_context(|| format!("Failed to bind HTTP server on {addr}"))?;

        Ok(tokio::spawn(async move {
            axum::serve(listener, router)
                .with_graceful_shutdown(async move {
                    let _ = shutdown.recv().await;
                    info!("HTTP server received shutdown signal");
                })
                .await
                .context("HTTP server failed")?;

            info!("HTTP server stopped");
            Ok(())
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::OrchestratorConfig;

    #[tokio::test]
    async fn peers_are_not_dialed_over_tls_without_a_ca_to_verify_them() {
        // Only the leaf is configured. Trusting it for peers would pin every
        // peer to this one certificate; refusing turns relaying off instead.
        let cfg = TlsServerConfig {
            cert_path: "/nonexistent/server.pem".to_string(),
            key_path: "/nonexistent/server.key".to_string(),
            ca_cert_path: None,
            require_client_cert: false,
        };
        let error = build_peer_tls_config(&cfg)
            .await
            .expect_err("no CA, no peer TLS");
        assert!(error.to_string().contains("no CA configured"), "{error:#}");
    }

    /// A gRPC server serving only `workflow_service`, with this module's
    /// limits, on a free local port; and the address it listens on.
    async fn workflow_server(
        max_message_bytes: usize,
        workflow_service: WorkflowService,
        shutdown: broadcast::Receiver<()>,
    ) -> String {
        use orcher_core::auth::NoAuthenticator;
        use orcher_core::grpc::auth_layer::AuthMode;
        use std::sync::Arc;

        let free = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = free.local_addr().unwrap().to_string();
        drop(free);
        let mut config = crate::config::OrchestratorConfig::default().server;
        config.grpc_addr = addr.clone();
        let manager = GrpcServerManager {
            config,
            max_message_bytes,
            auth_layer: AuthLayer::new(Arc::new(NoAuthenticator), AuthMode::None),
            tls_config: None,
            execution_service: None,
            workflow_service: Some(workflow_service),
            query_service: None,
            actor_service: None,
            worker_service: None,
            namespace_service: None,
            loopback: None,
        };
        manager
            .start(shutdown)
            .await
            .expect("start the gRPC server");
        format!("http://{addr}")
    }

    /// A workflow service whose database is never reached: everything these
    /// tests send is answered before it would be.
    fn workflow_service(
        limits: orcher_core::grpc::payload_limits::PayloadLimits,
    ) -> WorkflowService {
        use orcher_core::engine::queues::outbox::OutboxQueue;
        use orcher_core::events::EventBus;
        use orcher_core::workflows::{JournalService, WorkflowExecutionService};
        use orcher_events::bus::EventBusConfig;
        use orcher_storage::repositories::domains::coordination::OutboxRepository;
        use orcher_storage::repositories::domains::journal::JournalRepository;
        use orcher_storage::repositories::domains::workflow_execution::WorkflowExecutionRepository;
        use std::sync::Arc;

        let pool = sqlx::postgres::PgPoolOptions::new()
            .acquire_timeout(std::time::Duration::from_millis(200))
            .connect_lazy("postgresql://nobody@127.0.0.1:1/unreachable")
            .unwrap();
        let exec_repo = Arc::new(WorkflowExecutionRepository::new(pool.clone()));
        let journal_repo = Arc::new(JournalRepository::new(pool.clone()));
        WorkflowService::new(
            Arc::new(WorkflowExecutionService::new(
                exec_repo.clone(),
                Arc::new(JournalService::new(journal_repo)),
                Arc::new(OutboxQueue::new(Arc::new(OutboxRepository::new(Arc::new(
                    pool.clone(),
                ))))),
                EventBus::new(EventBusConfig::default()),
                pool,
            )),
            exec_repo,
        )
        .with_payload_limits(limits)
    }

    /// Messages over tonic's 4 MiB default reach the handlers, and a payload
    /// over the payload limit is refused by name. Before the limits were
    /// configurable, a 5 MiB workflow input was refused by the transport with
    /// "decoded message length too large", and nothing said which payload or
    /// what the limit was.
    #[tokio::test]
    async fn the_grpc_server_takes_large_messages_and_refuses_large_payloads_by_name() {
        use orcher_core::grpc::payload_limits::{PayloadLimits, MIB};
        use orcher_proto::orcher::v1::workflow_service_client::WorkflowServiceClient;
        use orcher_proto::orcher::v1::{SendEventRequest, StartWorkflowRequest};

        let limits = PayloadLimits {
            max_message_bytes: 16 * MIB,
            payload_max_bytes: 6 * MIB,
        };
        let (_stop, stopped) = broadcast::channel(1);
        let address =
            workflow_server(limits.max_message_bytes, workflow_service(limits), stopped).await;
        let mut client = WorkflowServiceClient::connect(address)
            .await
            .expect("connect")
            .max_encoding_message_size(64 * MIB)
            .max_decoding_message_size(64 * MIB);
        // A JSON string `len` bytes long.
        let json_string = |len: usize| {
            let mut bytes = vec![b'x'; len];
            bytes[0] = b'"';
            bytes[len - 1] = b'"';
            bytes
        };
        let start = |input: Vec<u8>| StartWorkflowRequest {
            workflow_type: "Large".to_string(),
            task_queue: "large".to_string(),
            namespace: "default".to_string(),
            input,
            ..Default::default()
        };

        // Between the gRPC default and the payload limit: past the transport,
        // as far as the database this test has none of.
        let status = client
            .start_workflow(start(json_string(5 * MIB)))
            .await
            .expect_err("there is no database to start it in");
        // Answered by the handler: a status the transport made up — a reset
        // stream, a message refused as too long — carries its cause.
        assert!(
            std::error::Error::source(&status).is_none(),
            "refused by the transport: {status:?}"
        );
        assert_ne!(status.code(), tonic::Code::OutOfRange, "{status:?}");
        assert!(!status.message().contains("too large"), "{status:?}");

        // Over the payload limit, under the message limit: refused by name.
        let status = client
            .start_workflow(start(vec![b'1'; 7 * MIB]))
            .await
            .expect_err("a 7 MiB input was accepted");
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
        assert_eq!(
            status.message(),
            "workflow input is 7 MiB; the limit is 6 MiB (payload_max_bytes). \
             Store large data elsewhere and pass a reference."
        );

        let status = client
            .send_event(SendEventRequest {
                workflow_id: "wf".to_string(),
                namespace: "default".to_string(),
                event_name: "approve".to_string(),
                payload: vec![b'1'; 7 * MIB],
                ..Default::default()
            })
            .await
            .expect_err("a 7 MiB event was accepted");
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
        assert_eq!(
            status.message(),
            "the payload of event \"approve\" is 7 MiB; the limit is 6 MiB \
             (payload_max_bytes). Store large data elsewhere and pass a reference."
        );

        // Over the message limit: the transport refuses it, before any handler.
        let status = client
            .start_workflow(start(vec![b'1'; 17 * MIB]))
            .await
            .expect_err("a message over the limit was accepted");
        assert_ne!(status.code(), tonic::Code::InvalidArgument, "{status:?}");
    }

    #[test]
    fn test_http_server_manager_creation() {
        let config = OrchestratorConfig::default();
        let health = HealthManager::new();
        let _manager = HttpServerManager::new(config.server, health);
    }

    #[tokio::test]
    async fn test_http_server_start_and_shutdown() {
        let mut config = OrchestratorConfig::default();
        config.server.http_addr = "127.0.0.1:0".to_string(); // Use random port

        let (tx, rx) = broadcast::channel(1);
        let health = HealthManager::new();
        let manager = HttpServerManager::new(config.server, health);

        let handle = manager
            .start(rx)
            .await
            .expect("Failed to start HTTP server");

        // Give server time to start
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        // Trigger shutdown
        let _ = tx.send(());

        // Stopped by the signal, so the task reports success.
        let exit = tokio::time::timeout(tokio::time::Duration::from_secs(2), handle)
            .await
            .expect("HTTP server did not stop on the shutdown signal")
            .expect("HTTP server task panicked");
        assert!(exit.is_ok(), "{exit:?}");
    }

    /// A port someone else holds must fail startup. It used to panic inside the
    /// spawned task instead, after `start` had returned Ok, leaving an engine
    /// that reported ready with no health or metrics endpoint.
    #[tokio::test]
    async fn an_http_port_already_in_use_fails_start() {
        let taken = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut config = OrchestratorConfig::default();
        config.server.http_addr = taken.local_addr().unwrap().to_string();

        let (_tx, rx) = broadcast::channel(1);
        let manager = HttpServerManager::new(config.server, HealthManager::new());

        let error = manager
            .start(rx)
            .await
            .expect_err("start succeeded on a port that is already bound");
        assert!(
            format!("{error:#}").contains("Failed to bind HTTP server"),
            "{error:#}"
        );
    }
}
