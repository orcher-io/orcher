//! Registry Models
//!
//! This module contains data models for workflow registry operations in the REST API.
//! These models are used for request/response serialization and validation.

use orcher_common::types::time::Timestamptz;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use validator::Validate;

/// Request to register a new workflow package
#[derive(Debug, Clone, Serialize, Deserialize, Validate, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct RegisterWorkflowRequest {
    /// Package name (lowercase, alphanumeric, hyphens, underscores)
    #[validate(length(min = 1, max = 255), regex = "PACKAGE_NAME_REGEX")]
    pub package_name: String,

    /// Package version (semantic versioning)
    #[validate(length(min = 1, max = 50), regex = "SEMVER_REGEX")]
    pub package_version: String,

    /// Deployment model: "wasm", "`git_container`", or "service"
    #[validate(length(min = 1, max = 50))]
    pub deployment_model: String,

    /// Model-specific deployment configuration (JSON)
    pub deployment_source: serde_json::Value,

    /// Complete workflow manifest (JSON)
    pub manifest: serde_json::Value,

    /// Optional description
    #[validate(length(max = 1000))]
    pub description: Option<String>,

    /// Optional tags for categorization
    #[validate(length(max = 20))]
    pub tags: Option<Vec<String>>,
}

/// Response after successful workflow registration
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct RegisterWorkflowResponse {
    /// Registry entry ID
    pub id: String,

    /// Package name
    pub package_name: String,

    /// Package version
    pub package_version: String,

    /// Deployment model
    pub deployment_model: String,

    /// Status (e.g., "uploaded", "active", "inactive")
    pub status: String,

    /// Upload timestamp
    pub uploaded_at: Timestamptz,

    /// Success message
    pub message: String,
}

/// Request to upload a WASM workflow binary
#[derive(Debug, Clone, Serialize, Deserialize, Validate, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct UploadWasmRequest {
    /// Package name
    #[validate(length(min = 1, max = 255))]
    pub package_name: String,

    /// Package version
    #[validate(length(min = 1, max = 50))]
    pub package_version: String,

    /// Workflow manifest (JSON)
    pub manifest: serde_json::Value,

    /// Base64-encoded WASM binary
    #[validate(length(min = 1))]
    pub wasm_binary_base64: String,

    /// Optional description
    #[validate(length(max = 1000))]
    pub description: Option<String>,

    /// Optional tags
    pub tags: Option<Vec<String>>,
}

/// Query parameters for listing workflows
#[derive(Debug, Clone, Serialize, Deserialize, Validate, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ListWorkflowsQuery {
    /// Filter by package name (exact match)
    #[validate(length(max = 255))]
    pub package_name: Option<String>,

    /// Filter by deployment model
    #[validate(length(max = 50))]
    pub deployment_model: Option<String>,

    /// Filter by status
    #[validate(length(max = 50))]
    pub status: Option<String>,

    /// Search in package names and descriptions
    #[validate(length(max = 255))]
    pub search: Option<String>,

    /// Filter by uploader user ID
    pub uploaded_by: Option<String>,

    /// Filter by tags (comma-separated)
    pub tags: Option<String>,

    /// Include deleted entries
    #[serde(default)]
    pub include_deleted: bool,

    /// Page number (1-indexed)
    #[serde(default = "default_page")]
    #[validate(range(min = 1))]
    pub page: u32,

    /// Page size (items per page)
    #[serde(default = "default_page_size")]
    #[validate(range(min = 1, max = 100))]
    pub page_size: u32,
}

fn default_page() -> u32 {
    1
}

fn default_page_size() -> u32 {
    20
}

/// Response for listing workflows
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ListWorkflowsResponse {
    /// Workflow entries
    pub workflows: Vec<WorkflowEntry>,

    /// Pagination metadata
    pub pagination: PaginationMetadata,
}

/// Workflow registry entry
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct WorkflowEntry {
    /// Entry ID
    pub id: String,

    /// Package name
    pub package_name: String,

    /// Package version
    pub package_version: String,

    /// Optional description
    pub description: Option<String>,

    /// Deployment model
    pub deployment_model: String,

    /// Model-specific deployment source (JSON)
    pub deployment_source: serde_json::Value,

    /// Status
    pub status: String,

    /// Number of workflows in package
    pub workflows_count: i32,

    /// Number of tasks in package
    pub tasks_count: i32,

    /// Uploaded by user ID
    pub uploaded_by: String,

    /// Upload timestamp
    pub uploaded_at: Timestamptz,

    /// Deployed by user ID (if deployed)
    pub deployed_by: Option<String>,

    /// Deployment timestamp (if deployed)
    pub deployed_at: Option<Timestamptz>,

    /// Tags
    pub tags: Vec<String>,

    /// Created timestamp
    pub created_at: Timestamptz,

    /// Updated timestamp
    pub updated_at: Timestamptz,
}

/// Pagination metadata
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct PaginationMetadata {
    /// Current page number
    pub page: u32,

    /// Page size
    pub page_size: u32,

    /// Total number of items
    pub total_items: u64,

    /// Total number of pages
    pub total_pages: u32,

    /// Whether there is a next page
    pub has_next: bool,

    /// Whether there is a previous page
    pub has_previous: bool,
}

/// Request to deploy a workflow
#[derive(Debug, Clone, Serialize, Deserialize, Validate, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct DeployWorkflowRequest {
    /// Optional target environment
    #[validate(length(max = 100))]
    pub environment: Option<String>,

    /// Optional deployment notes
    #[validate(length(max = 1000))]
    pub notes: Option<String>,
}

/// Response after successful deployment
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct DeployWorkflowResponse {
    /// Registry entry ID
    pub id: String,

    /// Deployment status
    pub status: String,

    /// Deployed by user ID
    pub deployed_by: String,

    /// Deployment timestamp
    pub deployed_at: Timestamptz,

    /// Success message
    pub message: String,
}

/// Request to update workflow metadata
#[derive(Debug, Clone, Serialize, Deserialize, Validate, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct UpdateWorkflowRequest {
    /// Optional new description
    #[validate(length(max = 1000))]
    pub description: Option<String>,

    /// Optional new tags (replaces existing)
    #[validate(length(max = 20))]
    pub tags: Option<Vec<String>>,

    /// Optional new status
    #[validate(length(max = 50))]
    pub status: Option<String>,
}

/// Response with detailed workflow information
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct WorkflowDetailResponse {
    /// Workflow entry
    pub workflow: WorkflowEntry,

    /// Complete manifest
    pub manifest: serde_json::Value,

    /// Metadata
    pub metadata: serde_json::Value,
}

/// WASM deployment source details
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct WasmDeploymentSource {
    /// S3 bucket name
    pub s3_bucket: Option<String>,

    /// S3 object key
    pub s3_key: Option<String>,

    /// S3 region
    pub s3_region: Option<String>,

    /// Binary size in bytes
    pub size_bytes: u64,

    /// SHA-256 checksum
    pub sha256: String,

    /// WASM target (e.g., "wasm32-wasi")
    pub target: Option<String>,

    /// Enabled features
    pub features: Vec<String>,

    /// Build profile (e.g., "release", "debug")
    pub build_profile: Option<String>,

    /// Rust compiler version
    pub rustc_version: Option<String>,

    /// Cargo version
    pub cargo_version: Option<String>,
}

/// Git+Container deployment source details
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct GitContainerDeploymentSource {
    /// Git repository URL
    pub git_url: String,

    /// Git reference (branch, tag, or commit)
    pub git_ref: String,

    /// Commit SHA
    pub git_commit: Option<String>,

    /// Path within repository
    pub path: Option<String>,

    /// Whether credentials are required
    pub requires_credentials: bool,

    /// Container image name (if already built)
    pub image: Option<String>,

    /// Dockerfile path
    pub dockerfile: Option<String>,

    /// Build arguments
    pub build_args: Option<serde_json::Value>,
}

/// Service-oriented deployment source details
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ServiceDeploymentSource {
    /// Service endpoint URL
    pub endpoint_url: String,

    /// Protocol: "http" or "grpc"
    pub protocol: String,

    /// Authentication type: "none", "bearer", "`api_key`", "oauth2"
    pub auth_type: String,

    /// Health check URL
    pub health_check_url: Option<String>,

    /// Request timeout in milliseconds
    pub timeout_ms: u32,

    /// Retry configuration
    pub retry_config: Option<RetryConfig>,

    /// Circuit breaker configuration
    pub circuit_breaker_config: Option<CircuitBreakerConfig>,
}

/// Retry configuration
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct RetryConfig {
    /// Maximum number of retry attempts
    pub max_attempts: u32,

    /// Initial backoff duration in milliseconds
    pub backoff_ms: u64,

    /// Maximum backoff duration in milliseconds
    pub max_backoff_ms: u64,
}

/// Circuit breaker configuration
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct CircuitBreakerConfig {
    /// Failure threshold before opening circuit
    pub failure_threshold: u32,

    /// Success threshold to close circuit
    pub success_threshold: u32,

    /// Timeout duration in milliseconds
    pub timeout_ms: u64,
}

// Regex patterns for validation
lazy_static::lazy_static! {
    static ref PACKAGE_NAME_REGEX: regex::Regex =
        regex::Regex::new(r"^[a-z0-9][a-z0-9_-]*$").unwrap();
    static ref SEMVER_REGEX: regex::Regex =
        regex::Regex::new(r"^\d+\.\d+\.\d+(-[a-zA-Z0-9.-]+)?(\+[a-zA-Z0-9.-]+)?$").unwrap();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_valid_package_name() {
        assert!(PACKAGE_NAME_REGEX.is_match("my-workflows"));
        assert!(PACKAGE_NAME_REGEX.is_match("my_workflows"));
        assert!(PACKAGE_NAME_REGEX.is_match("myworkflows123"));
        assert!(!PACKAGE_NAME_REGEX.is_match("My-Workflows")); // Uppercase not allowed
        assert!(!PACKAGE_NAME_REGEX.is_match("-my-workflows")); // Cannot start with hyphen
    }

    #[test]
    fn test_valid_semver() {
        assert!(SEMVER_REGEX.is_match("1.0.0"));
        assert!(SEMVER_REGEX.is_match("1.2.3"));
        assert!(SEMVER_REGEX.is_match("1.0.0-alpha"));
        assert!(SEMVER_REGEX.is_match("1.0.0-beta.1"));
        assert!(SEMVER_REGEX.is_match("1.0.0+build.123"));
        assert!(!SEMVER_REGEX.is_match("1.0")); // Not enough parts
        assert!(!SEMVER_REGEX.is_match("v1.0.0")); // No 'v' prefix
    }

    #[test]
    fn test_list_workflows_query_defaults() {
        let query = ListWorkflowsQuery {
            package_name: None,
            deployment_model: None,
            status: None,
            search: None,
            uploaded_by: None,
            tags: None,
            include_deleted: false,
            page: default_page(),
            page_size: default_page_size(),
        };

        assert_eq!(query.page, 1);
        assert_eq!(query.page_size, 20);
    }
}
