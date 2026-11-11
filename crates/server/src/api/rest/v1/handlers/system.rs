//! System Information Handlers
//!
//! Reports the REST API's build metadata, live component health, and the
//! (non-sensitive) orchestrator configuration. Orchestrator connectivity
//! reflects the gRPC client state established at startup; database health
//! is probed live per request.

use axum::{extract::State, http::StatusCode, Json};
use serde::{Deserialize, Serialize};

use crate::state::AppState;

/// Logical name reported for this service.
const SERVICE_NAME: &str = "orcher-api";

/// System information response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SystemInfoResponse {
    /// Service name
    pub name: String,
    /// Service version (build)
    pub version: String,
    /// Seconds since the REST API started
    pub uptime_seconds: u64,
}

/// System status response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SystemStatusResponse {
    /// Overall status: "healthy" or "degraded"
    pub status: String,
    /// Component statuses
    pub components: Vec<ComponentStatus>,
}

/// Component status
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComponentStatus {
    /// Component name
    pub name: String,
    /// Component status
    pub status: String,
}

/// System configuration response (non-sensitive fields only)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SystemConfigResponse {
    /// Orchestrator gRPC endpoint
    pub orchestrator_endpoint: String,
    /// Default namespace
    pub namespace: String,
    /// Whether the orchestrator connection was established at startup
    pub orchestrator_connected: bool,
}

/// Get system information (name, build version, uptime).
pub async fn get_system_info(
    State(state): State<AppState>,
) -> (StatusCode, Json<SystemInfoResponse>) {
    let response = SystemInfoResponse {
        name: SERVICE_NAME.to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        uptime_seconds: state.uptime_secs(),
    };
    (StatusCode::OK, Json(response))
}

/// Get system status: the REST API, live database probe, and orchestrator connectivity.
pub async fn get_system_status(
    State(state): State<AppState>,
) -> (StatusCode, Json<SystemStatusResponse>) {
    // The REST API is healthy if it is serving this request.
    let mut components = vec![ComponentStatus {
        name: "api".to_string(),
        status: "healthy".to_string(),
    }];

    // Live database probe.
    let db_healthy = sqlx::query("SELECT 1").execute(state.pool()).await.is_ok();
    components.push(ComponentStatus {
        name: "database".to_string(),
        status: if db_healthy { "healthy" } else { "unhealthy" }.to_string(),
    });

    // Orchestrator connectivity (established at startup).
    let orchestrator_connected = state.orchestrator_connected();
    components.push(ComponentStatus {
        name: "orchestrator".to_string(),
        status: if orchestrator_connected {
            "connected"
        } else {
            "unavailable"
        }
        .to_string(),
    });

    let overall = if db_healthy && orchestrator_connected {
        "healthy"
    } else {
        "degraded"
    };

    (
        StatusCode::OK,
        Json(SystemStatusResponse {
            status: overall.to_string(),
            components,
        }),
    )
}

/// Get the REST API's build version.
pub async fn get_version() -> (StatusCode, Json<String>) {
    (StatusCode::OK, Json(env!("CARGO_PKG_VERSION").to_string()))
}

/// Get the current non-sensitive system configuration.
pub async fn get_configuration(
    State(state): State<AppState>,
) -> (StatusCode, Json<SystemConfigResponse>) {
    let cfg = state.orchestrator_config();
    let response = SystemConfigResponse {
        orchestrator_endpoint: cfg.endpoint.clone(),
        namespace: cfg.namespace.clone(),
        orchestrator_connected: state.orchestrator_connected(),
    };
    (StatusCode::OK, Json(response))
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlx::PgPool;

    /// Build an AppState backed by a lazy (never-connected) pool. Handlers that
    /// don't touch the database work fully; the database probe reports unhealthy.
    fn test_state() -> AppState {
        let pool = PgPool::connect_lazy("postgresql://localhost/test")
            .expect("lazy pool should construct without connecting");
        AppState::new_for_test(pool)
    }

    #[tokio::test]
    async fn test_get_system_info() {
        let (status, Json(response)) = get_system_info(State(test_state())).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(response.name, SERVICE_NAME);
        assert_eq!(response.version, env!("CARGO_PKG_VERSION"));
    }

    #[tokio::test]
    async fn test_get_system_status_degraded_without_backends() {
        // No live DB and no orchestrator client in test mode → degraded, 200 OK.
        let (status, Json(response)) = get_system_status(State(test_state())).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(response.status, "degraded");
        assert!(response
            .components
            .iter()
            .any(|c| c.name == "api" && c.status == "healthy"));
        assert!(response.components.iter().any(|c| c.name == "database"));
        assert!(response
            .components
            .iter()
            .any(|c| c.name == "orchestrator" && c.status == "unavailable"));
    }

    #[tokio::test]
    async fn test_get_version() {
        let (status, Json(version)) = get_version().await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(version, env!("CARGO_PKG_VERSION"));
    }

    #[tokio::test]
    async fn test_get_configuration() {
        let (status, Json(config)) = get_configuration(State(test_state())).await;
        assert_eq!(status, StatusCode::OK);
        // new_for_test uses OrchestratorConfig::development() and no client.
        assert_eq!(config.orchestrator_endpoint, "http://localhost:50051");
        assert_eq!(config.namespace, "default");
        assert!(!config.orchestrator_connected);
    }
}
