//! # API Versioning Support
//!
//! This module provides comprehensive API versioning capabilities for the ORCHER REST API.
//! It enables backward compatibility, smooth API evolution, and client migration support.
//!
//! ## Features
//!
//! - **Multiple Versioning Strategies**: URL path, header-based, query parameter
//! - **Version Detection**: Automatic version extraction from requests
//! - **Deprecation Management**: Mark versions as deprecated with sunset dates
//! - **Version Negotiation**: Client-server version compatibility checking
//! - **Migration Helpers**: Tools to help clients migrate between versions
//! - **Version Routing**: Middleware for version-based request routing
//!
//! ## Versioning Strategies
//!
//! ### 1. URL Path Versioning (Recommended)
//! ```text
//! GET /api/v1/users
//! GET /api/v2/users
//! ```
//!
//! ### 2. Header Versioning
//! ```text
//! GET /api/users
//! X-API-Version: 1
//! ```
//!
//! ### 3. Query Parameter Versioning
//! ```text
//! GET /api/users?version=1
//! ```
//!
//! ## Usage
//!
//! ```rust,ignore
//! use orcher_server::api::rest::versioning::{ApiVersion, VersionDetector, VersioningStrategy};
//!
//! // Detect version from request
//! let version = VersionDetector::new(VersioningStrategy::UrlPath)
//!     .detect(&request)?;
//!
//! // Check if version is deprecated
//! if version.is_deprecated() {
//!     // Add deprecation warning to response
//! }
//!
//! // Get migration guide
//! let guide = version.migration_guide_to(ApiVersion::V2);
//! ```
//!
//! ## Version Lifecycle
//!
//! ```text
//! Development → Beta → Stable → Deprecated → Sunset
//!                                    ↓
//!                            (6-12 months notice)
//! ```

use axum::{
    extract::Request,
    http::{header::HeaderName, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use orcher_common::types::time::Timestamptz;
use serde::{Deserialize, Serialize};
use std::{fmt, str::FromStr};

/// API version identifier
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum ApiVersion {
    /// Version 1 - Current stable version
    V1,
    /// Version 2 - Future version (planned)
    V2,
    /// Version 3 - Future version (planned)
    V3,
}

impl ApiVersion {
    /// Get all supported API versions
    #[must_use]
    pub fn all() -> Vec<Self> {
        vec![Self::V1, Self::V2, Self::V3]
    }

    /// Get all active (non-deprecated) versions
    #[must_use]
    pub fn active() -> Vec<Self> {
        Self::all()
            .into_iter()
            .filter(|v| !v.is_deprecated())
            .collect()
    }

    /// Get the current stable version
    #[must_use]
    pub fn current() -> Self {
        Self::V1
    }

    /// Get the latest version (including beta)
    #[must_use]
    pub fn latest() -> Self {
        Self::V1
    }

    /// Check if this version is deprecated
    #[must_use]
    pub fn is_deprecated(&self) -> bool {
        matches!(self.lifecycle_status(), VersionLifecycle::Deprecated { .. })
    }

    /// Check if this version is sunset (no longer supported)
    #[must_use]
    pub fn is_sunset(&self) -> bool {
        matches!(self.lifecycle_status(), VersionLifecycle::Sunset)
    }

    /// Get the lifecycle status of this version
    #[must_use]
    pub fn lifecycle_status(&self) -> VersionLifecycle {
        match self {
            Self::V1 => VersionLifecycle::Stable,
            Self::V2 => VersionLifecycle::Development,
            Self::V3 => VersionLifecycle::Development,
        }
    }

    /// Get deprecation information if version is deprecated
    #[must_use]
    pub fn deprecation_info(&self) -> Option<DeprecationInfo> {
        match self.lifecycle_status() {
            VersionLifecycle::Deprecated {
                sunset_date,
                replacement,
            } => Some(DeprecationInfo {
                version: *self,
                sunset_date,
                replacement,
                message: format!(
                    "API version {} is deprecated and will be sunset on {}. Please migrate to version {}.",
                    self,
                    sunset_date.format("%Y-%m-%d"),
                    replacement
                ),
            }),
            _ => None,
        }
    }

    /// Get migration guide to another version
    #[must_use]
    pub fn migration_guide_to(&self, target: ApiVersion) -> Option<MigrationGuide> {
        if self == &target {
            return None;
        }

        Some(MigrationGuide {
            from: *self,
            to: target,
            breaking_changes: self.breaking_changes_to(target),
            migration_steps: self.migration_steps_to(target),
            estimated_effort: self.estimate_migration_effort(target),
        })
    }

    /// Get breaking changes between versions
    fn breaking_changes_to(&self, target: ApiVersion) -> Vec<BreakingChange> {
        match (self, target) {
            (Self::V1, Self::V2) => vec![
                BreakingChange {
                    area: "Authentication".to_string(),
                    description: "JWT token format changed to include additional claims"
                        .to_string(),
                    impact: ChangeImpact::Medium,
                    mitigation: "Update JWT parsing logic to handle new claims".to_string(),
                },
                BreakingChange {
                    area: "User API".to_string(),
                    description: "User creation endpoint now requires email verification"
                        .to_string(),
                    impact: ChangeImpact::Low,
                    mitigation: "Add email verification flow to user creation".to_string(),
                },
            ],
            _ => vec![],
        }
    }

    /// Get migration steps between versions
    fn migration_steps_to(&self, target: ApiVersion) -> Vec<String> {
        match (self, target) {
            (Self::V1, Self::V2) => vec![
                "Update API base URL from /api/v1 to /api/v2".to_string(),
                "Update authentication flow to handle new JWT format".to_string(),
                "Add email verification handling for user creation".to_string(),
                "Test all endpoints with new version".to_string(),
                "Update client libraries to v2".to_string(),
            ],
            _ => vec!["No migration path available yet".to_string()],
        }
    }

    /// Estimate migration effort
    fn estimate_migration_effort(&self, target: ApiVersion) -> MigrationEffort {
        let breaking_changes = self.breaking_changes_to(target);
        let high_impact = breaking_changes
            .iter()
            .filter(|c| matches!(c.impact, ChangeImpact::High))
            .count();
        let medium_impact = breaking_changes
            .iter()
            .filter(|c| matches!(c.impact, ChangeImpact::Medium))
            .count();

        if high_impact > 0 || medium_impact > 3 {
            MigrationEffort::High
        } else if medium_impact > 0 {
            MigrationEffort::Medium
        } else {
            MigrationEffort::Low
        }
    }

    /// Get version number as integer
    #[must_use]
    pub fn as_u8(&self) -> u8 {
        match self {
            Self::V1 => 1,
            Self::V2 => 2,
            Self::V3 => 3,
        }
    }

    /// Get version path segment (e.g., "v1")
    #[must_use]
    pub fn path_segment(&self) -> &'static str {
        match self {
            Self::V1 => "v1",
            Self::V2 => "v2",
            Self::V3 => "v3",
        }
    }
}

impl fmt::Display for ApiVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.path_segment())
    }
}

impl FromStr for ApiVersion {
    type Err = VersionError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "v1" | "1" => Ok(Self::V1),
            "v2" | "2" => Ok(Self::V2),
            "v3" | "3" => Ok(Self::V3),
            _ => Err(VersionError::UnsupportedVersion(s.to_string())),
        }
    }
}

/// Version lifecycle status
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VersionLifecycle {
    /// In development, not recommended for production
    Development,
    /// Beta testing, may have breaking changes
    Beta,
    /// Stable and recommended for production
    Stable,
    /// Deprecated, will be removed on sunset date
    Deprecated {
        sunset_date: Timestamptz,
        replacement: ApiVersion,
    },
    /// No longer supported
    Sunset,
}

/// Versioning strategy
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VersioningStrategy {
    /// Extract version from URL path (e.g., /api/v1/users)
    UrlPath,
    /// Extract version from header (e.g., X-API-Version: 1)
    Header,
    /// Extract version from query parameter (e.g., ?version=1)
    QueryParameter,
    /// Try multiple strategies in order
    Auto,
}

/// Version detector for extracting API version from requests
#[derive(Clone)]
pub struct VersionDetector {
    strategy: VersioningStrategy,
    header_name: HeaderName,
    query_param_name: String,
}

impl VersionDetector {
    /// Create a new version detector with the specified strategy
    #[must_use]
    pub fn new(strategy: VersioningStrategy) -> Self {
        Self {
            strategy,
            header_name: HeaderName::from_static("x-api-version"),
            query_param_name: "version".to_string(),
        }
    }

    /// Create a detector with custom header name
    pub fn with_header_name(mut self, name: HeaderName) -> Self {
        self.header_name = name;
        self
    }

    /// Create a detector with custom query parameter name
    pub fn with_query_param_name(mut self, name: String) -> Self {
        self.query_param_name = name;
        self
    }

    /// Detect API version from request
    pub fn detect(&self, request: &Request) -> Result<ApiVersion, VersionError> {
        match self.strategy {
            VersioningStrategy::UrlPath => self.detect_from_path(request),
            VersioningStrategy::Header => self.detect_from_header(request),
            VersioningStrategy::QueryParameter => self.detect_from_query(request),
            VersioningStrategy::Auto => self.detect_auto(request),
        }
    }

    /// Detect version from URL path
    fn detect_from_path(&self, request: &Request) -> Result<ApiVersion, VersionError> {
        let path = request.uri().path();

        // Try to extract version from path like /api/v1/... or /v1/...
        for segment in path.split('/') {
            if segment.starts_with('v') {
                if let Ok(version) = ApiVersion::from_str(segment) {
                    return Ok(version);
                }
            }
        }

        Err(VersionError::VersionNotFound)
    }

    /// Detect version from header
    fn detect_from_header(&self, request: &Request) -> Result<ApiVersion, VersionError> {
        request
            .headers()
            .get(&self.header_name)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| ApiVersion::from_str(v).ok())
            .ok_or(VersionError::VersionNotFound)
    }

    /// Detect version from query parameter
    fn detect_from_query(&self, request: &Request) -> Result<ApiVersion, VersionError> {
        request
            .uri()
            .query()
            .and_then(|q| {
                q.split('&')
                    .find(|pair| pair.starts_with(&format!("{}=", self.query_param_name)))
                    .and_then(|pair| pair.split('=').nth(1))
            })
            .and_then(|v| ApiVersion::from_str(v).ok())
            .ok_or(VersionError::VersionNotFound)
    }

    /// Auto-detect version using multiple strategies
    fn detect_auto(&self, request: &Request) -> Result<ApiVersion, VersionError> {
        // Try URL path first (most common)
        if let Ok(version) = self.detect_from_path(request) {
            return Ok(version);
        }

        // Try header
        if let Ok(version) = self.detect_from_header(request) {
            return Ok(version);
        }

        // Try query parameter
        if let Ok(version) = self.detect_from_query(request) {
            return Ok(version);
        }

        // Default to current version if not specified
        Ok(ApiVersion::current())
    }
}

impl Default for VersionDetector {
    fn default() -> Self {
        Self::new(VersioningStrategy::Auto)
    }
}

/// Deprecation information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeprecationInfo {
    /// Deprecated version
    pub version: ApiVersion,
    /// Date when version will be sunset
    pub sunset_date: Timestamptz,
    /// Replacement version
    pub replacement: ApiVersion,
    /// Deprecation message
    pub message: String,
}

/// Migration guide between versions
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MigrationGuide {
    /// Source version
    pub from: ApiVersion,
    /// Target version
    pub to: ApiVersion,
    /// Breaking changes
    pub breaking_changes: Vec<BreakingChange>,
    /// Migration steps
    pub migration_steps: Vec<String>,
    /// Estimated migration effort
    pub estimated_effort: MigrationEffort,
}

/// Breaking change description
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BreakingChange {
    /// Area affected (e.g., "Authentication", "User API")
    pub area: String,
    /// Description of the change
    pub description: String,
    /// Impact level
    pub impact: ChangeImpact,
    /// Suggested mitigation
    pub mitigation: String,
}

/// Change impact level
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ChangeImpact {
    /// Low impact, easy to adapt
    Low,
    /// Medium impact, requires some changes
    Medium,
    /// High impact, significant changes required
    High,
}

/// Migration effort estimate
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MigrationEffort {
    /// Low effort (< 1 day)
    Low,
    /// Medium effort (1-3 days)
    Medium,
    /// High effort (> 3 days)
    High,
}

/// Version-related errors
#[derive(Debug, Clone, thiserror::Error)]
pub enum VersionError {
    /// Version not found in request
    #[error("API version not specified in request")]
    VersionNotFound,

    /// Unsupported version
    #[error("Unsupported API version: {0}")]
    UnsupportedVersion(String),

    /// Version is deprecated
    #[error("API version {0} is deprecated")]
    DeprecatedVersion(ApiVersion),

    /// Version is sunset
    #[error("API version {0} is no longer supported")]
    SunsetVersion(ApiVersion),

    /// Invalid version format
    #[error("Invalid version format: {0}")]
    InvalidFormat(String),
}

impl IntoResponse for VersionError {
    fn into_response(self) -> Response {
        let (status, code, message) = match &self {
            VersionError::VersionNotFound => (
                StatusCode::BAD_REQUEST,
                "VERSION_NOT_FOUND",
                "API version not specified. Please include version in URL (e.g., /api/v1/...)",
            ),
            VersionError::UnsupportedVersion(_) => (
                StatusCode::BAD_REQUEST,
                "UNSUPPORTED_VERSION",
                "The requested API version is not supported",
            ),
            VersionError::DeprecatedVersion(_) => (
                StatusCode::OK, // Still works, but deprecated
                "DEPRECATED_VERSION",
                "This API version is deprecated",
            ),
            VersionError::SunsetVersion(_) => (
                StatusCode::GONE,
                "SUNSET_VERSION",
                "This API version is no longer supported",
            ),
            VersionError::InvalidFormat(_) => (
                StatusCode::BAD_REQUEST,
                "INVALID_VERSION_FORMAT",
                "Invalid version format",
            ),
        };

        let body = Json(serde_json::json!({
            "error": {
                "code": code,
                "status": status.as_u16(),
                "message": message,
                "details": self.to_string(),
                "timestamp": Timestamptz::now().to_rfc3339(),
            }
        }));

        (status, body).into_response()
    }
}

/// Version compatibility checker
pub struct VersionCompatibility;

impl VersionCompatibility {
    /// Check if a client version is compatible with a server version
    #[must_use]
    pub fn is_compatible(client: ApiVersion, server: ApiVersion) -> bool {
        // Same version is always compatible
        if client == server {
            return true;
        }

        // Client can use older versions if server supports them
        client <= server && !client.is_sunset()
    }

    /// Get compatibility information
    #[must_use]
    pub fn check(client: ApiVersion, server: ApiVersion) -> CompatibilityResult {
        if client.is_sunset() {
            return CompatibilityResult::Incompatible {
                reason: format!("Client version {client} is no longer supported"),
            };
        }

        if client == server {
            return CompatibilityResult::FullyCompatible;
        }

        if client < server {
            if client.is_deprecated() {
                return CompatibilityResult::CompatibleWithWarning {
                    warning: format!(
                        "Client version {client} is deprecated. Please upgrade to {server}"
                    ),
                };
            }
            return CompatibilityResult::BackwardCompatible;
        }

        CompatibilityResult::Incompatible {
            reason: format!("Client version {client} is newer than server version {server}"),
        }
    }
}

/// Compatibility check result
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CompatibilityResult {
    /// Fully compatible (same version)
    FullyCompatible,
    /// Backward compatible (client older, still supported)
    BackwardCompatible,
    /// Compatible but with warnings (e.g., deprecated)
    CompatibleWithWarning { warning: String },
    /// Incompatible (cannot work together)
    Incompatible { reason: String },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_version_parsing() {
        assert_eq!(ApiVersion::from_str("v1").unwrap(), ApiVersion::V1);
        assert_eq!(ApiVersion::from_str("1").unwrap(), ApiVersion::V1);
        assert_eq!(ApiVersion::from_str("V1").unwrap(), ApiVersion::V1);
        assert!(ApiVersion::from_str("v99").is_err());
    }

    #[test]
    fn test_version_ordering() {
        assert!(ApiVersion::V1 < ApiVersion::V2);
        assert!(ApiVersion::V2 < ApiVersion::V3);
        assert!(ApiVersion::V1 <= ApiVersion::V1);
    }

    #[test]
    fn test_version_lifecycle() {
        let v1 = ApiVersion::V1;
        assert_eq!(v1.lifecycle_status(), VersionLifecycle::Stable);
        assert!(!v1.is_deprecated());
        assert!(!v1.is_sunset());
    }

    #[test]
    fn test_version_current() {
        let current = ApiVersion::current();
        assert_eq!(current, ApiVersion::V1);
    }

    #[test]
    fn test_version_path_segment() {
        assert_eq!(ApiVersion::V1.path_segment(), "v1");
        assert_eq!(ApiVersion::V2.path_segment(), "v2");
    }

    #[test]
    fn test_compatibility_same_version() {
        assert!(VersionCompatibility::is_compatible(
            ApiVersion::V1,
            ApiVersion::V1
        ));
    }

    #[test]
    fn test_compatibility_older_client() {
        assert!(VersionCompatibility::is_compatible(
            ApiVersion::V1,
            ApiVersion::V2
        ));
    }

    #[test]
    fn test_compatibility_check() {
        let result = VersionCompatibility::check(ApiVersion::V1, ApiVersion::V1);
        assert_eq!(result, CompatibilityResult::FullyCompatible);

        let result = VersionCompatibility::check(ApiVersion::V1, ApiVersion::V2);
        assert_eq!(result, CompatibilityResult::BackwardCompatible);
    }

    #[test]
    fn test_migration_guide() {
        let guide = ApiVersion::V1.migration_guide_to(ApiVersion::V2);
        assert!(guide.is_some());

        let guide = guide.unwrap();
        assert_eq!(guide.from, ApiVersion::V1);
        assert_eq!(guide.to, ApiVersion::V2);
        assert!(!guide.breaking_changes.is_empty());
        assert!(!guide.migration_steps.is_empty());
    }
}
