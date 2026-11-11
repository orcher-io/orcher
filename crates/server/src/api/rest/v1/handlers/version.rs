//! # Version Information Handlers
//!
//! Provides endpoints for API version information, deprecation status,
//! and migration guides.

use axum::{
    extract::{Path, State},
    response::{IntoResponse, Response},
    Json,
};
use serde::{Deserialize, Serialize};
use tracing::{debug, info};

use crate::api::rest::versioning::ApiVersion;
use crate::error::{Result, ServerError};
use crate::state::AppState;

/// Version information response
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct VersionInfo {
    /// Current stable version
    pub current_version: String,
    /// Latest version (including beta)
    pub latest_version: String,
    /// All supported versions
    pub supported_versions: Vec<String>,
    /// Deprecated versions
    pub deprecated_versions: Vec<DeprecatedVersionInfo>,
    /// Sunset versions
    pub sunset_versions: Vec<String>,
    /// API documentation URL
    pub documentation_url: String,
    /// Changelog URL
    pub changelog_url: String,
}

/// Deprecated version information
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DeprecatedVersionInfo {
    /// Version identifier
    pub version: String,
    /// Sunset date
    pub sunset_date: String,
    /// Replacement version
    pub replacement_version: String,
    /// Deprecation message
    pub message: String,
}

/// Migration guide response
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MigrationGuideResponse {
    /// Source version
    pub from_version: String,
    /// Target version
    pub to_version: String,
    /// Breaking changes
    pub breaking_changes: Vec<BreakingChangeInfo>,
    /// Migration steps
    pub migration_steps: Vec<String>,
    /// Estimated effort
    pub estimated_effort: String,
    /// Documentation URL
    pub documentation_url: String,
}

/// Breaking change information
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BreakingChangeInfo {
    /// Area affected
    pub area: String,
    /// Description
    pub description: String,
    /// Impact level
    pub impact: String,
    /// Mitigation strategy
    pub mitigation: String,
}

/// Get API version information
///
/// # Endpoint
/// `GET /api/version`
///
/// # Response
/// ```json
/// {
///   "currentVersion": "v1",
///   "latestVersion": "v1",
///   "supportedVersions": ["v1"],
///   "deprecatedVersions": [],
///   "sunsetVersions": [],
///   "documentationUrl": "https://docs.orcher.io/api",
///   "changelogUrl": "https://docs.orcher.io/changelog"
/// }
/// ```
pub async fn get_version_info(State(_state): State<AppState>) -> Result<Response> {
    info!("→ GET /api/version - Fetching version information");

    let current = ApiVersion::current();
    let latest = ApiVersion::latest();
    let all_versions = ApiVersion::all();

    // Get deprecated versions
    let deprecated_versions: Vec<DeprecatedVersionInfo> = all_versions
        .iter()
        .filter_map(|v| {
            if let Some(info) = v.deprecation_info() {
                Some(DeprecatedVersionInfo {
                    version: v.to_string(),
                    sunset_date: info.sunset_date.to_rfc3339(),
                    replacement_version: info.replacement.to_string(),
                    message: info.message,
                })
            } else {
                None
            }
        })
        .collect();

    // Get sunset versions
    let sunset_versions: Vec<String> = all_versions
        .iter()
        .filter(|v| v.is_sunset())
        .map(std::string::ToString::to_string)
        .collect();

    // Get active (supported) versions
    let supported_versions: Vec<String> = ApiVersion::active()
        .iter()
        .map(std::string::ToString::to_string)
        .collect();

    let response = VersionInfo {
        current_version: current.to_string(),
        latest_version: latest.to_string(),
        supported_versions,
        deprecated_versions,
        sunset_versions,
        documentation_url: "https://docs.orcher.io/api".to_string(),
        changelog_url: "https://docs.orcher.io/changelog".to_string(),
    };

    debug!("← 200 OK - Version information");
    Ok(Json(response).into_response())
}

/// Get specific version information
///
/// # Endpoint
/// `GET /api/version/:version`
///
/// # Parameters
/// - `version` - Version identifier (e.g., "v1", "v2")
///
/// # Response
/// ```json
/// {
///   "version": "v1",
///   "status": "stable",
///   "releaseDate": "2024-01-01T00:00:00Z",
///   "deprecated": false,
///   "sunset": false,
///   "features": ["users", "workflows", "tasks"],
///   "documentation": "https://docs.orcher.io/api/v1"
/// }
/// ```
pub async fn get_specific_version_info(
    State(_state): State<AppState>,
    Path(version): Path<String>,
) -> Result<Response> {
    info!(
        "→ GET /api/version/{} - Fetching specific version information",
        version
    );

    // Parse version
    let api_version = version
        .parse::<ApiVersion>()
        .map_err(|_| ServerError::not_found(format!("Version '{version}' not found")))?;

    let lifecycle = api_version.lifecycle_status();
    let status_str = match lifecycle {
        crate::api::rest::versioning::VersionLifecycle::Development => "development",
        crate::api::rest::versioning::VersionLifecycle::Beta => "beta",
        crate::api::rest::versioning::VersionLifecycle::Stable => "stable",
        crate::api::rest::versioning::VersionLifecycle::Deprecated { .. } => "deprecated",
        crate::api::rest::versioning::VersionLifecycle::Sunset => "sunset",
    };

    let deprecation_info = api_version.deprecation_info();

    let response = serde_json::json!({
        "version": api_version.to_string(),
        "status": status_str,
        "deprecated": api_version.is_deprecated(),
        "sunset": api_version.is_sunset(),
        "deprecationInfo": deprecation_info.map(|info| serde_json::json!({
            "sunsetDate": info.sunset_date.to_rfc3339(),
            "replacementVersion": info.replacement.to_string(),
            "message": info.message,
        })),
        "documentation": format!("https://docs.orcher.io/api/{}", api_version.path_segment()),
    });

    debug!("← 200 OK - Version {} information", version);
    Ok(Json(response).into_response())
}

/// Get migration guide between versions
///
/// # Endpoint
/// `GET /api/version/migration/:from/:to`
///
/// # Parameters
/// - `from` - Source version (e.g., "v1")
/// - `to` - Target version (e.g., "v2")
///
/// # Response
/// ```json
/// {
///   "fromVersion": "v1",
///   "toVersion": "v2",
///   "breakingChanges": [
///     {
///       "area": "Authentication",
///       "description": "JWT format changed",
///       "impact": "Medium",
///       "mitigation": "Update JWT parsing logic"
///     }
///   ],
///   "migrationSteps": [
///     "Update API base URL",
///     "Update authentication flow"
///   ],
///   "estimatedEffort": "Medium",
///   "documentationUrl": "https://docs.orcher.io/api/migration/v1-to-v2"
/// }
/// ```
pub async fn get_migration_guide(
    State(_state): State<AppState>,
    Path((from, to)): Path<(String, String)>,
) -> Result<Response> {
    info!(
        "→ GET /api/version/migration/{}/{} - Fetching migration guide",
        from, to
    );

    // Parse versions
    let from_version = from
        .parse::<ApiVersion>()
        .map_err(|_| ServerError::not_found(format!("Version '{from}' not found")))?;

    let to_version = to
        .parse::<ApiVersion>()
        .map_err(|_| ServerError::not_found(format!("Version '{to}' not found")))?;

    // Get migration guide
    let guide = from_version.migration_guide_to(to_version).ok_or_else(|| {
        ServerError::not_found(format!("No migration guide available from {from} to {to}"))
    })?;

    let response = MigrationGuideResponse {
        from_version: guide.from.to_string(),
        to_version: guide.to.to_string(),
        breaking_changes: guide
            .breaking_changes
            .into_iter()
            .map(|change| BreakingChangeInfo {
                area: change.area,
                description: change.description,
                impact: format!("{:?}", change.impact),
                mitigation: change.mitigation,
            })
            .collect(),
        migration_steps: guide.migration_steps,
        estimated_effort: format!("{:?}", guide.estimated_effort),
        documentation_url: format!(
            "https://docs.orcher.io/api/migration/{}-to-{}",
            guide.from.path_segment(),
            guide.to.path_segment()
        ),
    };

    debug!("← 200 OK - Migration guide from {} to {}", from, to);
    Ok(Json(response).into_response())
}

/// Check version compatibility
///
/// # Endpoint
/// `GET /api/version/compatibility/:client/:server`
///
/// # Parameters
/// - `client` - Client version (e.g., "v1")
/// - `server` - Server version (e.g., "v2")
///
/// # Response
/// ```json
/// {
///   "compatible": true,
///   "status": "BackwardCompatible",
///   "message": "Client version is compatible with server",
///   "recommendation": "Consider upgrading to latest version"
/// }
/// ```
pub async fn check_compatibility(
    State(_state): State<AppState>,
    Path((client, server)): Path<(String, String)>,
) -> Result<Response> {
    info!(
        "→ GET /api/version/compatibility/{}/{} - Checking compatibility",
        client, server
    );

    // Parse versions
    let client_version = client
        .parse::<ApiVersion>()
        .map_err(|_| ServerError::not_found(format!("Version '{client}' not found")))?;

    let server_version = server
        .parse::<ApiVersion>()
        .map_err(|_| ServerError::not_found(format!("Version '{server}' not found")))?;

    // Check compatibility
    use crate::api::rest::versioning::{CompatibilityResult, VersionCompatibility};

    let is_compatible = VersionCompatibility::is_compatible(client_version, server_version);
    let compat_result = VersionCompatibility::check(client_version, server_version);

    let (status, message, recommendation) = match &compat_result {
        CompatibilityResult::FullyCompatible => (
            "FullyCompatible",
            "Client and server versions are identical".to_string(),
            Some("Consider upgrading to the latest version for new features".to_string()),
        ),
        CompatibilityResult::BackwardCompatible => (
            "BackwardCompatible",
            "Client version is compatible with server".to_string(),
            Some("Consider upgrading to the latest version for new features".to_string()),
        ),
        CompatibilityResult::CompatibleWithWarning { warning } => (
            "CompatibleWithWarning",
            warning.clone(),
            Some("Please plan migration to avoid future incompatibility".to_string()),
        ),
        CompatibilityResult::Incompatible { reason } => (
            "Incompatible",
            reason.clone(),
            Some("Migration required before using this server version".to_string()),
        ),
    };

    let response = serde_json::json!({
        "compatible": is_compatible,
        "status": status,
        "clientVersion": client_version.to_string(),
        "serverVersion": server_version.to_string(),
        "message": message,
        "recommendation": recommendation,
    });

    debug!(
        "← 200 OK - Compatibility check: client={}, server={}, compatible={}",
        client, server, is_compatible
    );
    Ok(Json(response).into_response())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_version_info_serialization() {
        let info = VersionInfo {
            current_version: "v1".to_string(),
            latest_version: "v1".to_string(),
            supported_versions: vec!["v1".to_string()],
            deprecated_versions: vec![],
            sunset_versions: vec![],
            documentation_url: "https://docs.orcher.io/api".to_string(),
            changelog_url: "https://docs.orcher.io/changelog".to_string(),
        };

        let json = serde_json::to_string(&info).unwrap();
        assert!(json.contains("currentVersion"));
        assert!(json.contains("v1"));
    }
}
