//! Health Check Handlers
//!
//! This module implements health check endpoints for monitoring service availability.
//! These endpoints are typically used by orchestrators (Kubernetes, Docker Swarm)
//! and monitoring systems to determine service health.

use axum::{extract::State, http::StatusCode, Json};
use orcher_common::types::time::Timestamptz;

use crate::state::AppState;

/// Health check response
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct HealthResponse {
    /// Service status ("healthy", "degraded", "unhealthy")
    pub status: String,
    /// Timestamp of the health check
    pub timestamp: String,
    /// Service version
    pub version: String,
}

/// Liveness probe handler
///
/// Indicates whether the service is alive and running.
/// Returns 200 OK if the service is alive, regardless of dependencies.
///
/// # Returns
///
/// Returns a simple health status response.
pub async fn liveness() -> (StatusCode, Json<HealthResponse>) {
    let response = HealthResponse {
        status: "healthy".to_string(),
        timestamp: Timestamptz::now().to_rfc3339(),
        version: env!("CARGO_PKG_VERSION").to_string(),
    };
    (StatusCode::OK, Json(response))
}

/// Readiness probe handler
///
/// Indicates whether the service is ready to accept traffic. The gateway is
/// ready only when it can reach its dependencies: the database (probed live)
/// and the orchestrator gRPC connection (established at startup).
///
/// # Returns
///
/// `200 OK` with status `"healthy"` when ready, or `503 Service Unavailable`
/// with status `"unavailable"` when a dependency is missing — so load
/// balancers and Kubernetes stop routing traffic to an unready instance.
pub async fn readiness(State(state): State<AppState>) -> (StatusCode, Json<HealthResponse>) {
    let db_healthy = sqlx::query("SELECT 1").execute(state.pool()).await.is_ok();
    let orchestrator_connected = state.orchestrator_connected();
    let ready = db_healthy && orchestrator_connected;

    let (code, status) = if ready {
        (StatusCode::OK, "healthy")
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, "unavailable")
    };

    let response = HealthResponse {
        status: status.to_string(),
        timestamp: Timestamptz::now().to_rfc3339(),
        version: env!("CARGO_PKG_VERSION").to_string(),
    };
    (code, Json(response))
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlx::PgPool;

    /// AppState backed by a lazy (never-connected) pool: the database probe
    /// fails and no orchestrator client is present, so readiness is not ready.
    fn test_state() -> AppState {
        let pool = PgPool::connect_lazy("postgresql://localhost/test")
            .expect("lazy pool should construct without connecting");
        AppState::new_for_test(pool)
    }

    #[tokio::test]
    async fn test_liveness() {
        let (status, Json(response)) = liveness().await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(response.status, "healthy");
        assert_eq!(response.version, env!("CARGO_PKG_VERSION"));
    }

    #[tokio::test]
    async fn test_readiness_unavailable_without_backends() {
        // No live DB and no orchestrator client in test mode → not ready.
        let (status, Json(response)) = readiness(State(test_state())).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(response.status, "unavailable");
    }
}
