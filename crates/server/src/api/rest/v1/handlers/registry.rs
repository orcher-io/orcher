//! Registry Handlers - DISABLED (Code-First Model)
//!
//! **CODE-FIRST ONLY MODEL**
//!
//! All platform-based workflow registration and deployment endpoints are disabled.
//! These handlers now return errors indicating the feature is not supported.
//!
//! ## Migration to Code-First
//!
//! Use the `#[workflow]` macro to register workflows at compile-time:
//!
//! ```rust,ignore
//! #[workflow(name = "my_workflow", version = "1.0.0")]
//! async fn my_workflow(ctx: WorkflowContext) -> Result<()> {
//!     // Automatically registered in GLOBAL_REGISTRY
//! }
//! ```

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde_json::json;
use tracing::info;
use uuid::Uuid;

use crate::api::rest::extractors::{AuditContext, AuthUser};
use crate::error::{Result, ServerError};
use crate::state::AppState;

/// Register a new workflow package - DISABLED
///
/// # Endpoint
/// `POST /api/v1/registry/workflows`
///
/// Returns an error indicating platform registration is not supported.
#[axum::debug_handler]
pub async fn register_workflow(
    State(_state): State<AppState>,
    _audit_ctx: AuditContext,
    AuthUser(_user): AuthUser,
    Json(_request): Json<serde_json::Value>,
) -> Result<Response> {
    info!("→ POST /api/v1/registry/workflows - DISABLED (Code-First Model)");

    Err(ServerError::NotImplemented(
        "Platform-based workflow registration is disabled. \
         Use compile-time registration via #[workflow] macro instead."
            .to_string(),
    ))
}

/// Upload WASM binary - DISABLED
///
/// # Endpoint
/// `POST /api/v1/registry/workflows/wasm`
///
/// Returns an error indicating WASM upload is not supported.
#[axum::debug_handler]
pub async fn upload_wasm(
    State(_state): State<AppState>,
    _audit_ctx: AuditContext,
    AuthUser(_user): AuthUser,
    Json(_request): Json<serde_json::Value>,
) -> Result<Response> {
    info!("→ POST /api/v1/registry/workflows/wasm - DISABLED (Code-First Model)");

    Err(ServerError::NotImplemented(
        "WASM upload is disabled in code-first model. \
         Use compile-time registration via #[workflow] macro instead."
            .to_string(),
    ))
}

/// List workflows - DISABLED
///
/// # Endpoint
/// `GET /api/v1/registry/workflows`
///
/// Returns an error indicating platform registry is not supported.
#[axum::debug_handler]
pub async fn list_workflows(
    State(_state): State<AppState>,
    AuthUser(_user): AuthUser,
    Query(_query): Query<serde_json::Value>,
) -> Result<Response> {
    info!("→ GET /api/v1/registry/workflows - DISABLED (Code-First Model)");

    Err(ServerError::NotImplemented(
        "Platform registry listing is disabled. \
         Workflows are registered at compile-time via #[workflow] macro."
            .to_string(),
    ))
}

/// Get workflow by ID - DISABLED
///
/// # Endpoint
/// `GET /api/v1/registry/workflows/:id`
///
/// Returns an error indicating platform registry is not supported.
#[axum::debug_handler]
pub async fn get_workflow(
    State(_state): State<AppState>,
    AuthUser(_user): AuthUser,
    Path(_id): Path<Uuid>,
) -> Result<Response> {
    info!("→ GET /api/v1/registry/workflows/:id - DISABLED (Code-First Model)");

    Err(ServerError::NotImplemented(
        "Platform registry lookups are disabled. \
         Use code-first workflows registered via #[workflow] macro."
            .to_string(),
    ))
}

/// Update workflow - DISABLED
///
/// # Endpoint
/// `PUT /api/v1/registry/workflows/:id`
///
/// Returns an error indicating platform registry is not supported.
#[axum::debug_handler]
pub async fn update_workflow(
    State(_state): State<AppState>,
    _audit_ctx: AuditContext,
    AuthUser(_user): AuthUser,
    Path(_id): Path<Uuid>,
    Json(_request): Json<serde_json::Value>,
) -> Result<Response> {
    info!("→ PUT /api/v1/registry/workflows/:id - DISABLED (Code-First Model)");

    Err(ServerError::NotImplemented(
        "Platform registry updates are disabled in code-first model.".to_string(),
    ))
}

/// Delete workflow - DISABLED
///
/// # Endpoint
/// `DELETE /api/v1/registry/workflows/:id`
///
/// Returns an error indicating platform registry is not supported.
#[axum::debug_handler]
pub async fn delete_workflow(
    State(_state): State<AppState>,
    _audit_ctx: AuditContext,
    AuthUser(_user): AuthUser,
    Path(_id): Path<Uuid>,
) -> Result<Response> {
    info!("→ DELETE /api/v1/registry/workflows/:id - DISABLED (Code-First Model)");

    Err(ServerError::NotImplemented(
        "Platform registry operations are disabled in code-first model.".to_string(),
    ))
}

/// Deploy workflow - DISABLED
///
/// # Endpoint
/// `POST /api/v1/registry/workflows/:id/deploy`
///
/// Returns an error indicating platform deployment is not supported.
#[axum::debug_handler]
pub async fn deploy_workflow(
    State(_state): State<AppState>,
    _audit_ctx: AuditContext,
    AuthUser(_user): AuthUser,
    Path(_id): Path<Uuid>,
    Json(_request): Json<serde_json::Value>,
) -> Result<Response> {
    info!("→ POST /api/v1/registry/workflows/:id/deploy - DISABLED (Code-First Model)");

    Err(ServerError::NotImplemented(
        "Platform-based workflow deployment is disabled. \
         Deploy your service binary directly to your infrastructure (K8s/ECS/Docker)."
            .to_string(),
    ))
}

/// Health check for registry endpoints
///
/// # Endpoint
/// `GET /api/v1/registry/health`
///
/// Returns information about the registry status.
#[axum::debug_handler]
pub async fn health_check() -> Result<Response> {
    Ok((
        StatusCode::OK,
        Json(json!({
            "status": "ok",
            "registry_type": "code-first",
            "platform_registry": "disabled",
            "message": "Registry uses compile-time workflow registration via #[workflow] macro"
        })),
    )
        .into_response())
}

#[cfg(test)]
mod tests {

    #[test]
    fn test_module_compiles() {
        // Smoke test to ensure module compiles
        assert!(true);
    }
}
