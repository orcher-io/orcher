//! HTTP Server Implementation
//!
//! This module implements the main HTTP server using Axum framework.
//! It handles server initialization, configuration, graceful shutdown,
//! and lifecycle management.

use crate::error::Result;
use crate::state::AppState;
use axum::{body::Body, http::Request, middleware::Next, Router};
use std::net::SocketAddr;
use tokio::net::TcpListener;

use axum::http::{HeaderName, HeaderValue, Method};
use tower_http::{
    compression::CompressionLayer,
    cors::{AllowOrigin, CorsLayer},
    trace::{DefaultMakeSpan, DefaultOnResponse, TraceLayer},
    LatencyUnit,
};

use crate::middleware::versioning;
use tracing::{info, warn, Level};

/// HTTP Server configuration
#[derive(Debug, Clone)]
pub struct ServerConfig {
    /// Server bind address
    pub host: String,
    /// Server bind port
    pub port: u16,
    /// Enable compression
    pub enable_compression: bool,
    /// Enable CORS
    pub enable_cors: bool,
    /// Enable request tracing
    pub enable_tracing: bool,
    /// Emit security response headers via the configured `SecurityPolicy`
    pub enable_security_headers: bool,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            host: "0.0.0.0".to_string(),
            port: 8080,
            enable_compression: true,
            enable_cors: true,
            enable_tracing: true,
            enable_security_headers: true,
        }
    }
}

/// HTTP Server instance
pub struct Server {
    config: ServerConfig,
    state: AppState,
    router: Option<Router>,
    extra_routes: Vec<Router<AppState>>,
}

impl Server {
    /// Create a new server with the given configuration
    ///
    /// # Arguments
    ///
    /// * `config` - Server configuration
    /// * `state` - Application state with service dependencies
    ///
    /// # Returns
    ///
    /// Returns a new server instance.
    #[must_use]
    pub fn new(config: ServerConfig, state: AppState) -> Self {
        Self {
            config,
            state,
            router: None,
            extra_routes: Vec::new(),
        }
    }

    /// Create a new server with default configuration
    ///
    /// # Arguments
    ///
    /// * `state` - Application state with service dependencies
    ///
    /// # Returns
    ///
    /// Returns a new server instance with default configuration.
    #[must_use]
    pub fn with_defaults(state: AppState) -> Self {
        Self::new(ServerConfig::default(), state)
    }

    /// Add additional routes to be merged under `/api/v1`.
    ///
    /// Used by the cloud gateway to inject account routes (auth, orgs, users, etc.)
    /// without modifying the core server.
    pub fn merge_routes(&mut self, routes: Router<AppState>) {
        self.extra_routes.push(routes);
    }

    /// The REST API with all its middleware, for a process that serves it on
    /// a listener of its own, as the engine does on its HTTP port.
    #[must_use]
    pub fn router(&self) -> Router {
        self.build_router()
    }

    /// Build the router with all middleware and routes
    ///
    /// # Returns
    ///
    /// Returns a configured Axum router.
    fn build_router(&self) -> Router {
        // Add API routes under /api/v1 prefix
        let mut v1_routes = crate::api::rest::v1::routes::configure_routes();

        // Merge extra routes (e.g., account routes from cloud gateway)
        for extra in &self.extra_routes {
            v1_routes = v1_routes.merge(extra.clone());
        }

        // Add middleware to inject AppState into request extensions
        // This makes it available to other middleware that need it
        let state_for_extension = self.state.clone();
        let v1_with_extension = v1_routes.layer(axum::middleware::from_fn(
            move |mut request: Request<Body>, next: Next| {
                let state = state_for_extension.clone();
                async move {
                    request.extensions_mut().insert(state);
                    next.run(request).await
                }
            },
        ));

        let mut router = Router::new().nest("/api/v1", v1_with_extension);

        // A request body as large as the engine accepts in one message, in
        // place of axum's 2 MB default: a workflow's input arrives here whole,
        // and the engine, not this layer, says when a payload is too large.
        router = router.layer(axum::extract::DefaultBodyLimit::max(
            orcher_core::grpc::payload_limits::max_message_bytes_from_env(),
        ));

        // Add versioning middleware (must be before other middleware to detect version)
        router = router.layer(axum::middleware::from_fn(versioning::versioning_middleware));

        // Apply security response headers via the pluggable SecurityPolicy.
        // OSS uses DefaultSecurityPolicy; cloud / self-hosters inject their own
        // via AppState::set_security_policy.
        if self.config.enable_security_headers {
            let state_for_security = self.state.clone();
            router = router.layer(axum::middleware::from_fn(
                move |request: Request<Body>, next: Next| {
                    let state = state_for_security.clone();
                    async move {
                        let mut response = next.run(request).await;
                        state
                            .security_policy()
                            .apply_response_headers(response.headers_mut());
                        response
                    }
                },
            ));
        }

        // Add compression middleware if enabled
        if self.config.enable_compression {
            router = router.layer(CompressionLayer::new());
        }

        // Add CORS middleware if enabled
        if self.config.enable_cors {
            router = router.layer(Self::build_cors_layer());
        }

        // Add tracing middleware if enabled
        if self.config.enable_tracing {
            router = router.layer(
                TraceLayer::new_for_http()
                    .make_span_with(DefaultMakeSpan::new().level(Level::INFO))
                    .on_request(|request: &Request<Body>, _span: &tracing::Span| {
                        info!(
                            "→ {} {} {:?}",
                            request.method(),
                            request.uri(),
                            request.version()
                        );
                    })
                    .on_response(
                        DefaultOnResponse::new()
                            .level(Level::INFO)
                            .latency_unit(LatencyUnit::Millis),
                    ),
            );
        }

        // Inject application state
        router.with_state(self.state.clone())
    }

    /// Build a CORS layer from environment config.
    ///
    /// Reads `CORS_ALLOWED_ORIGINS` (comma-separated). If set to `"*"`, allows all origins.
    /// Otherwise defaults to `localhost:3000` and `localhost:8080`.
    fn build_cors_layer() -> CorsLayer {
        let origins_env = std::env::var("CORS_ALLOWED_ORIGINS").unwrap_or_default();

        let cors = if origins_env.trim() == "*" {
            info!("CORS: allowing all origins (CORS_ALLOWED_ORIGINS=*)");
            CorsLayer::permissive()
        } else {
            let origins: Vec<String> = if origins_env.is_empty() {
                vec![
                    "http://localhost:3000".to_string(),
                    "http://localhost:8080".to_string(),
                ]
            } else {
                origins_env
                    .split(',')
                    .map(|s| s.trim().to_string())
                    .collect()
            };

            info!("CORS: allowed origins: {:?}", origins);

            let allow_origins: Vec<HeaderValue> =
                origins.iter().filter_map(|o| o.parse().ok()).collect();

            CorsLayer::new()
                .allow_origin(AllowOrigin::list(allow_origins))
                .allow_methods([
                    Method::GET,
                    Method::POST,
                    Method::PUT,
                    Method::DELETE,
                    Method::PATCH,
                    Method::OPTIONS,
                ])
                .allow_headers([
                    HeaderName::from_static("content-type"),
                    HeaderName::from_static("authorization"),
                    HeaderName::from_static("x-request-id"),
                    HeaderName::from_static("x-api-key"),
                ])
                .allow_credentials(true)
                .max_age(std::time::Duration::from_secs(3600))
        };

        cors
    }

    /// Initialize the server
    ///
    /// This prepares the server by building the router and
    /// configuring all middleware and routes.
    pub fn init(&mut self) -> Result<()> {
        info!(
            "Initializing HTTP server on {}:{}",
            self.config.host, self.config.port
        );

        let router = self.build_router();
        self.router = Some(router);

        info!("HTTP server initialized successfully");
        Ok(())
    }

    /// Start the server and listen for incoming connections
    ///
    /// This method blocks until the server is shut down.
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` on successful shutdown, or an error if
    /// the server fails to start or encounters a fatal error.
    pub async fn run(&self) -> Result<()> {
        let router = self
            .router
            .as_ref()
            .ok_or_else(|| crate::error::ServerError::ServerNotInitialized)?;

        let addr = format!("{}:{}", self.config.host, self.config.port);
        let socket_addr: SocketAddr = addr
            .parse()
            .map_err(|e| crate::error::ServerError::InvalidAddress(format!("{e}")))?;

        info!("Starting HTTP server on {}", socket_addr);

        let listener = TcpListener::bind(socket_addr)
            .await
            .map_err(|e| crate::error::ServerError::BindError(format!("{e}")))?;

        info!("HTTP server listening on {}", socket_addr);

        axum::serve(listener, router.clone())
            .await
            .map_err(|e| crate::error::ServerError::ServeError(format!("{e}")))?;

        info!("HTTP server shut down gracefully");
        Ok(())
    }

    /// Start the server with graceful shutdown support
    ///
    /// This method starts the server and listens for shutdown signals
    /// (SIGINT, SIGTERM) to gracefully shut down the server.
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` on successful shutdown.
    pub async fn run_with_graceful_shutdown(&self) -> Result<()> {
        let router = self
            .router
            .as_ref()
            .ok_or_else(|| crate::error::ServerError::ServerNotInitialized)?;

        let addr = format!("{}:{}", self.config.host, self.config.port);
        let socket_addr: SocketAddr = addr
            .parse()
            .map_err(|e| crate::error::ServerError::InvalidAddress(format!("{e}")))?;

        info!("Starting HTTP server on {}", socket_addr);

        let listener = TcpListener::bind(socket_addr)
            .await
            .map_err(|e| crate::error::ServerError::BindError(format!("{e}")))?;

        info!("HTTP server listening on {}", socket_addr);

        // Set up graceful shutdown signal handler
        axum::serve(listener, router.clone())
            .with_graceful_shutdown(shutdown_signal())
            .await
            .map_err(|e| crate::error::ServerError::ServeError(format!("{e}")))?;

        info!("HTTP server shut down gracefully");
        Ok(())
    }
}

/// Wait for shutdown signal (SIGINT or SIGTERM)
async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install signal handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        () = ctrl_c => {
            warn!("Received SIGINT, shutting down gracefully...");
        },
        () = terminate => {
            warn!("Received SIGTERM, shutting down gracefully...");
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlx::PgPool;

    // Helper function to create a mock AppState for testing
    fn create_test_app_state() -> AppState {
        // Create a mock pool (this won't actually connect to a database in tests)
        let pool = PgPool::connect_lazy("postgresql://localhost/test")
            .expect("Failed to create test pool");

        // Use the test constructor
        AppState::new_for_test(pool)
    }

    #[test]
    fn test_default_config() {
        let config = ServerConfig::default();
        assert_eq!(config.host, "0.0.0.0");
        assert_eq!(config.port, 8080);
        assert!(config.enable_compression);
        assert!(config.enable_cors);
        assert!(config.enable_tracing);
    }

    #[test]
    fn test_custom_config() {
        let config = ServerConfig {
            host: "127.0.0.1".to_string(),
            port: 3000,
            enable_compression: false,
            enable_cors: false,
            enable_tracing: false,
            enable_security_headers: false,
        };
        assert_eq!(config.host, "127.0.0.1");
        assert_eq!(config.port, 3000);
        assert!(!config.enable_compression);
        assert!(!config.enable_cors);
        assert!(!config.enable_tracing);
    }

    #[tokio::test]
    async fn test_server_creation() {
        let state = create_test_app_state();
        let server = Server::with_defaults(state);
        assert_eq!(server.config.port, 8080);
    }

    #[tokio::test]
    async fn test_server_init() {
        let state = create_test_app_state();
        let mut server = Server::with_defaults(state);
        let result = server.init();
        assert!(result.is_ok());
        assert!(server.router.is_some());
    }
}
