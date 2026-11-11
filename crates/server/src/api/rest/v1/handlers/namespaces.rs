//! Namespace Handlers
//!
//! This module implements REST API handlers for namespace management operations.
//! All operations communicate with the NamespaceService via gRPC.

use crate::error::{Result, ServerError};
use crate::state::AppState;
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use serde::{Deserialize, Serialize};
use tracing::{debug, info};

/// Query parameters for listing namespaces
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListNamespacesQuery {
    /// Maximum number of results
    #[serde(default = "default_limit")]
    pub limit: i32,

    /// Offset for pagination
    #[serde(default)]
    pub offset: i32,
}

fn default_limit() -> i32 {
    100
}

/// Request body for creating a namespace
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateNamespaceRequest {
    /// Unique namespace name
    pub name: String,

    /// Human-readable description
    #[serde(default)]
    pub description: String,

    /// How long completed workflow data is retained (in days)
    #[serde(default = "default_retention")]
    pub retention_period_days: i32,

    /// Email of the namespace owner
    #[serde(default)]
    pub owner_email: String,
}

fn default_retention() -> i32 {
    30
}

/// Request body for updating a namespace
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UpdateNamespaceRequest {
    /// Updated description (omit or empty to leave unchanged)
    #[serde(default)]
    pub description: String,

    /// Updated retention period in days (0 means no change)
    #[serde(default)]
    pub retention_period_days: i32,
}

/// Namespace response returned to callers
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct NamespaceResponse {
    pub name: String,
    pub status: String,
    pub description: String,
    pub owner_email: String,
    pub retention_period_days: i32,
    pub created_at: Option<String>,
    pub updated_at: Option<String>,
}

/// List of namespaces with total count
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ListNamespacesResponse {
    pub namespaces: Vec<NamespaceResponse>,
    pub total: i64,
    pub count: usize,
    pub offset: i32,
}

/// Format a protobuf Timestamp as an RFC 3339 / ISO-8601 string (UTC).
fn format_timestamp(ts: prost_types::Timestamp) -> String {
    orcher_common::types::time::Timestamptz::from_second(ts.seconds)
        .map(|t| t.to_rfc3339())
        .unwrap_or_default()
}

/// Convert proto NamespaceInfo to REST response
fn proto_to_response(ns: orcher_proto::orcher::v1::NamespaceInfo) -> NamespaceResponse {
    NamespaceResponse {
        name: ns.name,
        status: ns.status,
        description: ns.description,
        owner_email: ns.owner_email,
        retention_period_days: ns.retention_period_days,
        created_at: ns.created_at.map(format_timestamp),
        updated_at: ns.updated_at.map(format_timestamp),
    }
}

/// Get the namespace gRPC client from state or return 503
macro_rules! namespace_client {
    ($state:expr) => {
        $state.namespace_client().ok_or_else(|| {
            ServerError::service_unavailable(
                "Namespace service is unavailable (orchestrator not connected)",
            )
        })?
    };
}

/// List namespaces
///
/// # Request
///
/// GET /api/v1/namespaces?limit=100&offset=0
///
/// # Response
///
/// ```json
/// {
///   "namespaces": [{ "name": "default", "status": "active", ... }],
///   "total": 1,
///   "count": 1,
///   "offset": 0
/// }
/// ```
pub async fn list_namespaces(
    State(state): State<AppState>,
    Query(mut query): Query<ListNamespacesQuery>,
) -> Result<Json<ListNamespacesResponse>> {
    // Clamp client-supplied pagination to a safe range (see MAX_PAGE_SIZE).
    query.limit = query.limit.clamp(1, super::MAX_PAGE_SIZE as i32);
    query.offset = query.offset.max(0);

    debug!(
        limit = query.limit,
        offset = query.offset,
        "Listing namespaces"
    );

    let client = namespace_client!(state);
    let (namespaces, total) = client
        .list_namespaces(query.limit, query.offset)
        .await
        .map_err(|e| ServerError::internal(format!("Failed to list namespaces: {}", e)))?;

    let count = namespaces.len();
    let response = ListNamespacesResponse {
        namespaces: namespaces.into_iter().map(proto_to_response).collect(),
        total,
        count,
        offset: query.offset,
    };

    Ok(Json(response))
}

/// Get a namespace by name
///
/// # Request
///
/// GET /api/v1/namespaces/:name
///
/// # Response
///
/// ```json
/// { "name": "default", "status": "active", "description": "...", ... }
/// ```
pub async fn get_namespace(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Result<Json<NamespaceResponse>> {
    debug!(name = %name, "Getting namespace");

    let client = namespace_client!(state);
    let ns = client.get_namespace(&name).await.map_err(|e| {
        let msg = e.to_string();
        if msg.to_lowercase().contains("not found") {
            ServerError::not_found(format!("Namespace not found: {}", name))
        } else {
            ServerError::internal(format!("Failed to get namespace: {}", e))
        }
    })?;

    Ok(Json(proto_to_response(ns)))
}

/// Create a namespace
///
/// # Request
///
/// POST /api/v1/namespaces
///
/// ```json
/// {
///   "name": "my-namespace",
///   "description": "A test namespace",
///   "retentionPeriodDays": 30,
///   "ownerEmail": "owner@example.com"
/// }
/// ```
///
/// # Response
///
/// Returns 201 Created with the new namespace details.
pub async fn create_namespace(
    State(state): State<AppState>,
    Json(request): Json<CreateNamespaceRequest>,
) -> Result<(StatusCode, Json<NamespaceResponse>)> {
    info!(name = %request.name, "Creating namespace");

    let client = namespace_client!(state);
    let ns = client
        .create_namespace(
            &request.name,
            &request.description,
            request.retention_period_days,
            &request.owner_email,
        )
        .await
        .map_err(|e| ServerError::internal(format!("Failed to create namespace: {}", e)))?;

    info!(name = %ns.name, "Namespace created successfully");
    Ok((StatusCode::CREATED, Json(proto_to_response(ns))))
}

/// Update a namespace
///
/// # Request
///
/// PUT /api/v1/namespaces/:name
///
/// ```json
/// {
///   "description": "Updated description",
///   "retentionPeriodDays": 60
/// }
/// ```
///
/// # Response
///
/// Returns 200 OK with the updated namespace details.
pub async fn update_namespace(
    State(state): State<AppState>,
    Path(name): Path<String>,
    Json(request): Json<UpdateNamespaceRequest>,
) -> Result<Json<NamespaceResponse>> {
    info!(name = %name, "Updating namespace");

    let client = namespace_client!(state);
    let ns = client
        .update_namespace(&name, &request.description, request.retention_period_days)
        .await
        .map_err(|e| {
            let msg = e.to_string();
            if msg.to_lowercase().contains("not found") {
                ServerError::not_found(format!("Namespace not found: {}", name))
            } else {
                ServerError::internal(format!("Failed to update namespace: {}", e))
            }
        })?;

    info!(name = %ns.name, "Namespace updated successfully");
    Ok(Json(proto_to_response(ns)))
}

/// Delete a namespace (soft-delete)
///
/// # Request
///
/// DELETE /api/v1/namespaces/:name
///
/// # Response
///
/// Returns 204 No Content on success.
pub async fn delete_namespace(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Result<StatusCode> {
    info!(name = %name, "Deleting namespace");

    let client = namespace_client!(state);
    client.delete_namespace(&name).await.map_err(|e| {
        let msg = e.to_string();
        if msg.to_lowercase().contains("not found") {
            ServerError::not_found(format!("Namespace not found: {}", name))
        } else {
            ServerError::internal(format!("Failed to delete namespace: {}", e))
        }
    })?;

    info!(name = %name, "Namespace deleted successfully");
    Ok(StatusCode::NO_CONTENT)
}

/// Deprecate a namespace
///
/// Marks a namespace as deprecated — no new workflows can be started in it.
///
/// # Request
///
/// POST /api/v1/namespaces/:name/deprecate
///
/// # Response
///
/// Returns 200 OK with the deprecated namespace details.
pub async fn deprecate_namespace(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Result<Json<NamespaceResponse>> {
    info!(name = %name, "Deprecating namespace");

    let client = namespace_client!(state);
    let ns = client.deprecate_namespace(&name).await.map_err(|e| {
        let msg = e.to_string();
        if msg.to_lowercase().contains("not found") {
            ServerError::not_found(format!("Namespace not found: {}", name))
        } else {
            ServerError::internal(format!("Failed to deprecate namespace: {}", e))
        }
    })?;

    info!(name = %ns.name, "Namespace deprecated successfully");
    Ok(Json(proto_to_response(ns)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_limit() {
        assert_eq!(default_limit(), 100);
    }

    #[test]
    fn test_default_retention() {
        assert_eq!(default_retention(), 30);
    }
}
