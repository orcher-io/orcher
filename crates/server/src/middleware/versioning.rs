//! # API Versioning Middleware
//!
//! Provides middleware for automatic API version detection, validation, and deprecation warnings.
//!
//! ## Features
//!
//! - Automatic version detection from URL path, headers, or query parameters
//! - Version validation and lifecycle checking
//! - Deprecation warnings in response headers
//! - Version sunset handling (410 Gone)
//! - Version information injection into request extensions
//!
//! ## Usage
//!
//! ```rust,ignore
//! use orcher_server::middleware::versioning::{VersioningMiddleware, VersioningConfig};
//!
//! let versioning = VersioningMiddleware::new(VersioningConfig::default());
//! let app = Router::new()
//!     .route("/users", get(list_users))
//!     .layer(axum::middleware::from_fn(versioning.process));
//! ```

use axum::{
    extract::Request,
    http::{header, HeaderValue, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
    Json,
};
use serde_json::json;
use tracing::{debug, info, warn};

use crate::api::rest::versioning::{ApiVersion, VersionDetector, VersionError, VersioningStrategy};
use orcher_common::types::time::Timestamptz;

/// Versioning middleware configuration
#[derive(Debug, Clone)]
pub struct VersioningConfig {
    /// Versioning strategy to use
    pub strategy: VersioningStrategy,

    /// Whether to require explicit version in requests
    pub require_version: bool,

    /// Whether to add version headers to responses
    pub add_version_headers: bool,

    /// Whether to add deprecation warnings
    pub add_deprecation_warnings: bool,

    /// Whether to reject sunset versions
    pub reject_sunset: bool,

    /// Default version if not specified
    pub default_version: Option<ApiVersion>,
}

impl Default for VersioningConfig {
    fn default() -> Self {
        Self {
            strategy: VersioningStrategy::Auto,
            require_version: false,
            add_version_headers: true,
            add_deprecation_warnings: true,
            reject_sunset: true,
            default_version: Some(ApiVersion::current()),
        }
    }
}

/// Versioning middleware
#[derive(Clone)]
pub struct VersioningMiddleware {
    config: VersioningConfig,
    detector: VersionDetector,
}

impl VersioningMiddleware {
    /// Create a new versioning middleware with the given configuration
    #[must_use]
    pub fn new(config: VersioningConfig) -> Self {
        let detector = VersionDetector::new(config.strategy);
        Self { config, detector }
    }

    /// Process request with versioning
    pub async fn process(&self, mut request: Request, next: Next) -> Result<Response, Response> {
        // Detect API version from request
        let version = match self.detect_version(&request) {
            Ok(v) => v,
            Err(e) => {
                warn!("Version detection failed: {}", e);
                return Err(self.handle_version_error(e));
            }
        };

        debug!("Detected API version: {}", version);

        // Validate version lifecycle
        if let Err(e) = self.validate_version(version) {
            warn!("Version validation failed for {}: {}", version, e);
            return Err(self.handle_version_error(e));
        }

        // Store version in request extensions for handlers
        request.extensions_mut().insert(version);

        // Process request
        let mut response = next.run(request).await;

        // Add version information to response headers
        if self.config.add_version_headers {
            self.add_version_headers(&mut response, version);
        }

        // Add deprecation warnings if needed
        if self.config.add_deprecation_warnings && version.is_deprecated() {
            self.add_deprecation_headers(&mut response, version);
        }

        Ok(response)
    }

    /// Detect version from request
    fn detect_version(&self, request: &Request) -> Result<ApiVersion, VersionError> {
        match self.detector.detect(request) {
            Ok(version) => Ok(version),
            Err(VersionError::VersionNotFound) => {
                // If version not found and we have a default, use it
                if !self.config.require_version {
                    if let Some(default) = self.config.default_version {
                        debug!("Using default version: {}", default);
                        return Ok(default);
                    }
                }
                Err(VersionError::VersionNotFound)
            }
            Err(e) => Err(e),
        }
    }

    /// Validate version lifecycle
    fn validate_version(&self, version: ApiVersion) -> Result<(), VersionError> {
        // Check if version is sunset
        if self.config.reject_sunset && version.is_sunset() {
            return Err(VersionError::SunsetVersion(version));
        }

        Ok(())
    }

    /// Add version headers to response
    fn add_version_headers(&self, response: &mut Response, version: ApiVersion) {
        let headers = response.headers_mut();

        // Add X-API-Version header
        if let Ok(value) = HeaderValue::from_str(&version.to_string()) {
            headers.insert("X-API-Version", value);
        }

        // Add version number
        if let Ok(value) = HeaderValue::from_str(&version.as_u8().to_string()) {
            headers.insert("X-API-Version-Number", value);
        }
    }

    /// Add deprecation headers to response
    fn add_deprecation_headers(&self, response: &mut Response, version: ApiVersion) {
        if let Some(info) = version.deprecation_info() {
            let headers = response.headers_mut();

            // Add X-API-Deprecated header
            headers.insert("X-API-Deprecated", HeaderValue::from_static("true"));

            // Add X-API-Sunset-Date header
            if let Ok(value) = HeaderValue::from_str(&info.sunset_date.to_rfc3339()) {
                headers.insert("X-API-Sunset-Date", value);
            }

            // Add X-API-Replacement-Version header
            if let Ok(value) = HeaderValue::from_str(&info.replacement.to_string()) {
                headers.insert("X-API-Replacement-Version", value);
            }

            // Add standard Sunset header (RFC 8594)
            if let Ok(value) = HeaderValue::from_str(
                &info
                    .sunset_date
                    .format("%a, %d %b %Y %H:%M:%S GMT")
                    .to_string(),
            ) {
                headers.insert(header::HeaderName::from_static("sunset"), value);
            }

            // Add Deprecation header (RFC 8594)
            headers.insert(
                header::HeaderName::from_static("deprecation"),
                HeaderValue::from_static("true"),
            );

            // Add Link header with migration guide
            let link = format!(
                r#"<https://docs.orcher.io/api/migration/{}-to-{}>; rel="deprecation""#,
                version.path_segment(),
                info.replacement.path_segment()
            );
            if let Ok(value) = HeaderValue::from_str(&link) {
                headers.insert(header::LINK, value);
            }

            info!(
                "Added deprecation headers for version {} (sunset: {})",
                version, info.sunset_date
            );
        }
    }

    /// Handle version error
    fn handle_version_error(&self, error: VersionError) -> Response {
        match &error {
            VersionError::SunsetVersion(version) => {
                warn!("Request to sunset version: {}", version);

                // Return 410 Gone for sunset versions
                let body = Json(json!({
                    "error": {
                        "code": "VERSION_SUNSET",
                        "status": 410,
                        "message": format!("API version {} is no longer supported", version),
                        "details": {
                            "sunset_version": version.to_string(),
                            "current_version": ApiVersion::current().to_string(),
                            "migration_guide": format!(
                                "https://docs.orcher.io/api/migration/{}-to-{}",
                                version.path_segment(),
                                ApiVersion::current().path_segment()
                            )
                        },
                        "timestamp": Timestamptz::now().to_rfc3339(),
                    }
                }));

                (StatusCode::GONE, body).into_response()
            }
            VersionError::DeprecatedVersion(version) => {
                // Deprecated versions still work, just add warnings
                // This shouldn't normally be returned as an error
                warn!("Request to deprecated version: {}", version);
                error.into_response()
            }
            _ => {
                // Other version errors (not found, unsupported, invalid format)
                warn!("Version error: {}", error);
                error.into_response()
            }
        }
    }
}

/// Axum middleware function for versioning
pub async fn versioning_middleware(request: Request, next: Next) -> Result<Response, Response> {
    let middleware = VersioningMiddleware::new(VersioningConfig::default());
    middleware.process(request, next).await
}

/// Axum middleware function with custom config
pub fn versioning_middleware_with_config(
    config: VersioningConfig,
) -> impl Fn(
    Request,
    Next,
)
    -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Response, Response>> + Send>>
       + Clone {
    let middleware = VersioningMiddleware::new(config);
    move |request: Request, next: Next| {
        let middleware = middleware.clone();
        Box::pin(async move { middleware.process(request, next).await })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = VersioningConfig::default();
        assert!(!config.require_version);
        assert!(config.add_version_headers);
        assert!(config.add_deprecation_warnings);
        assert!(config.reject_sunset);
        assert_eq!(config.default_version, Some(ApiVersion::current()));
    }

    #[test]
    fn test_versioning_middleware_creation() {
        let config = VersioningConfig::default();
        let _middleware = VersioningMiddleware::new(config);
        // Should create without panicking
    }
}
