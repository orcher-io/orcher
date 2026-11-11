//! Error types for the ORCHER server crate

use axum::{
    extract::rejection::JsonRejection,
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use orcher_common::error::OrcherError;
use serde::{Deserialize, Serialize};

use orcher_common::types::time::Timestamptz;
use std::fmt;

/// Result type alias for server operations
pub type Result<T> = std::result::Result<T, ServerError>;

/// Error types that can occur in the server
#[derive(Debug, thiserror::Error)]
pub enum ServerError {
    /// gRPC client error
    #[error("gRPC error: {0}")]
    Grpc(#[from] tonic::Status),

    /// Configuration error
    #[error("Configuration error: {0}")]
    Config(String),

    /// IO error
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    /// Internal server error
    #[error("Internal error: {0}")]
    Internal(String),

    /// Server not initialized error
    #[error("Server not initialized - call init() before run()")]
    ServerNotInitialized,

    /// Invalid address error
    #[error("Invalid address: {0}")]
    InvalidAddress(String),

    /// Bind error
    #[error("Failed to bind to address: {0}")]
    BindError(String),

    /// Serve error
    #[error("Server error: {0}")]
    ServeError(String),

    /// Validation error
    #[error("Validation error: {0}")]
    Validation(String),

    /// Not found error
    #[error("Resource not found: {0}")]
    NotFound(String),

    /// Unauthorized error
    #[error("Unauthorized: {0}")]
    Unauthorized(String),

    /// Forbidden error
    #[error("Forbidden: {0}")]
    Forbidden(String),

    /// Conflict error
    #[error("Conflict: {0}")]
    Conflict(String),

    /// Bad request error
    #[error("Bad request: {0}")]
    BadRequest(String),

    /// Service unavailable error
    #[error("Service unavailable: {0}")]
    ServiceUnavailable(String),

    /// Not implemented (permanently removed or unsupported feature)
    #[error("Not implemented: {0}")]
    NotImplemented(String),

    /// `OrcherError` wrapper
    #[error("Orcher error: {0}")]
    Orcher(#[from] OrcherError),

    /// JSON deserialization error
    #[error("Invalid JSON: {0}")]
    JsonRejection(String),
}

impl ServerError {
    /// Create a new configuration error
    pub fn config(msg: impl fmt::Display) -> Self {
        Self::Config(msg.to_string())
    }

    /// Create a new internal error
    pub fn internal(msg: impl fmt::Display) -> Self {
        Self::Internal(msg.to_string())
    }

    /// Create a new validation error
    pub fn validation(msg: impl fmt::Display) -> Self {
        Self::Validation(msg.to_string())
    }

    /// Create a new not found error
    pub fn not_found(msg: impl fmt::Display) -> Self {
        Self::NotFound(msg.to_string())
    }

    /// Create a new bad request error
    pub fn bad_request(msg: impl fmt::Display) -> Self {
        Self::BadRequest(msg.to_string())
    }

    /// Create a new service unavailable error
    pub fn service_unavailable(msg: impl fmt::Display) -> Self {
        Self::ServiceUnavailable(msg.to_string())
    }

    /// Create a new not-implemented error
    pub fn not_implemented(msg: impl fmt::Display) -> Self {
        Self::NotImplemented(msg.to_string())
    }

    /// Create a new unauthorized error
    pub fn unauthorized(msg: impl fmt::Display) -> Self {
        Self::Unauthorized(msg.to_string())
    }

    /// Create a new forbidden error
    pub fn forbidden(msg: impl fmt::Display) -> Self {
        Self::Forbidden(msg.to_string())
    }

    /// Create a new conflict error
    pub fn conflict(msg: impl fmt::Display) -> Self {
        Self::Conflict(msg.to_string())
    }

    /// Create an email not verified error
    #[must_use]
    pub fn email_not_verified() -> Self {
        Self::Unauthorized(
            "Email address not verified. Please check your inbox for a verification email."
                .to_string(),
        )
    }

    /// Create a missing authentication token error with helpful guidance
    #[must_use]
    pub fn missing_auth_token() -> Self {
        Self::Unauthorized(
            "Authentication required. Send a credential, an API key or a token, as \
             `Authorization: Bearer <credential>`."
                .to_string(),
        )
    }

    /// Create an invalid token error with specific reason
    pub fn invalid_token(reason: impl fmt::Display) -> Self {
        Self::Unauthorized(format!(
            "Authentication failed: {reason}. Please login again to obtain a valid token."
        ))
    }

    /// Create a token expired error with helpful action
    #[must_use]
    pub fn token_expired() -> Self {
        Self::Unauthorized(
            "Your authentication token has expired. Please login again to continue.".to_string(),
        )
    }

    /// The engine's answer to a call, as the REST API's: a refusal keeps its
    /// meaning and the engine's explanation (an unknown namespace is a 404,
    /// a bad argument a 400), and only a failure the caller cannot act on is
    /// an internal error.
    #[must_use]
    pub fn from_status(status: tonic::Status) -> Self {
        use tonic::Code;
        let message = status.message().to_string();
        match status.code() {
            Code::NotFound => Self::NotFound(message),
            Code::InvalidArgument | Code::OutOfRange => Self::Validation(message),
            Code::FailedPrecondition => Self::BadRequest(message),
            Code::AlreadyExists | Code::Aborted => Self::Conflict(message),
            Code::PermissionDenied => Self::Forbidden(message),
            Code::Unauthenticated => Self::Unauthorized(message),
            Code::Unavailable | Code::ResourceExhausted => Self::ServiceUnavailable(message),
            Code::Unimplemented => Self::NotImplemented(message),
            _ => Self::Grpc(status),
        }
    }

    /// Convert `OrcherError` to `ServerError`
    #[must_use]
    pub fn from_orcher_error(error: OrcherError) -> Self {
        Self::Orcher(error)
    }

    /// Get the appropriate HTTP status code for this error
    /// The message that is safe to return to a client.
    ///
    /// User-facing (4xx-ish) variants carry our own messages and pass through;
    /// variants that wrap raw internal/DB/gRPC error text are replaced with a
    /// generic message so implementation details never reach the client.
    fn client_message(&self) -> String {
        match self {
            Self::Validation(_)
            | Self::NotFound(_)
            | Self::Unauthorized(_)
            | Self::Forbidden(_)
            | Self::Conflict(_)
            | Self::BadRequest(_)
            | Self::ServiceUnavailable(_)
            | Self::NotImplemented(_)
            | Self::JsonRejection(_) => self.to_string(),
            _ => match self.status_code() {
                StatusCode::BAD_GATEWAY => "Upstream service error".to_string(),
                StatusCode::SERVICE_UNAVAILABLE => "Service temporarily unavailable".to_string(),
                _ => "Internal server error".to_string(),
            },
        }
    }

    pub fn status_code(&self) -> StatusCode {
        match self {
            Self::Grpc(_) => StatusCode::BAD_GATEWAY,
            Self::Config(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Self::Io(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Self::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Self::ServerNotInitialized => StatusCode::INTERNAL_SERVER_ERROR,
            Self::InvalidAddress(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Self::BindError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Self::ServeError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Self::Validation(_) => StatusCode::BAD_REQUEST,
            Self::NotFound(_) => StatusCode::NOT_FOUND,
            Self::Unauthorized(_) => StatusCode::UNAUTHORIZED,
            Self::Forbidden(_) => StatusCode::FORBIDDEN,
            Self::Conflict(_) => StatusCode::CONFLICT,
            Self::BadRequest(_) => StatusCode::BAD_REQUEST,
            Self::ServiceUnavailable(_) => StatusCode::SERVICE_UNAVAILABLE,
            Self::NotImplemented(_) => StatusCode::NOT_IMPLEMENTED,
            Self::Orcher(e) => match e {
                OrcherError::NotFound { .. } => StatusCode::NOT_FOUND,
                OrcherError::Validation { .. } => StatusCode::BAD_REQUEST,
                OrcherError::Authentication { .. } => StatusCode::UNAUTHORIZED,
                OrcherError::Authorization { .. } => StatusCode::FORBIDDEN,
                _ => StatusCode::INTERNAL_SERVER_ERROR,
            },
            Self::JsonRejection(_) => StatusCode::BAD_REQUEST,
        }
    }

    /// Get the error code string for this error
    pub fn error_code(&self) -> &'static str {
        match self {
            Self::Grpc(_) => "GRPC_ERROR",
            Self::Config(_) => "CONFIG_ERROR",
            Self::Io(_) => "IO_ERROR",
            Self::Internal(_) => "INTERNAL_ERROR",
            Self::ServerNotInitialized => "SERVER_NOT_INITIALIZED",
            Self::InvalidAddress(_) => "INVALID_ADDRESS",
            Self::BindError(_) => "BIND_ERROR",
            Self::ServeError(_) => "SERVE_ERROR",
            Self::Validation(_) => "VALIDATION_ERROR",
            Self::NotFound(_) => "NOT_FOUND",
            Self::Unauthorized(_) => "UNAUTHORIZED",
            Self::Forbidden(_) => "FORBIDDEN",
            Self::Conflict(_) => "CONFLICT",
            Self::BadRequest(_) => "BAD_REQUEST",
            Self::ServiceUnavailable(_) => "SERVICE_UNAVAILABLE",
            Self::NotImplemented(_) => "NOT_IMPLEMENTED",
            Self::Orcher(e) => match e {
                OrcherError::NotFound { .. } => "NOT_FOUND",
                OrcherError::Validation { .. } => "VALIDATION_ERROR",
                OrcherError::Authentication { .. } => "UNAUTHORIZED",
                OrcherError::Authorization { .. } => "FORBIDDEN",
                _ => "INTERNAL_ERROR",
            },
            Self::JsonRejection(_) => "INVALID_JSON",
        }
    }

    /// Get detailed error information
    pub fn details(&self) -> Option<ErrorDetails> {
        match self {
            Self::Validation(msg) => Some(ErrorDetails {
                field: None,
                message: msg.clone(),
                suggestion: Some("Please check your input and try again".to_string()),
                action: None,
            }),
            Self::Unauthorized(msg) if msg.contains("Email address not verified") => {
                Some(ErrorDetails {
                    field: Some("email_verified".to_string()),
                    message: "Your email address has not been verified yet.".to_string(),
                    suggestion: Some(
                        "Check your inbox for a verification email or request a new one."
                            .to_string(),
                    ),
                    action: Some(ErrorAction {
                        action_type: "resend_verification".to_string(),
                        endpoint: Some("/api/v1/auth/resend-verification".to_string()),
                        method: Some("POST".to_string()),
                        data: None,
                    }),
                })
            }
            Self::Unauthorized(msg) if msg.contains("Authentication required") => {
                Some(ErrorDetails {
                    field: Some("authorization".to_string()),
                    message: "No authentication token provided.".to_string(),
                    suggestion: Some(
                        "Include a Bearer token in the Authorization header: 'Authorization: Bearer <your-token>'"
                            .to_string(),
                    ),
                    action: Some(ErrorAction {
                        action_type: "login".to_string(),
                        endpoint: Some("/api/v1/auth/login".to_string()),
                        method: Some("POST".to_string()),
                        data: Some(serde_json::json!({
                            "email": "your-email@example.com",
                            "password": "your-password"
                        })),
                    }),
                })
            }
            Self::Unauthorized(msg) if msg.contains("token has expired") => {
                Some(ErrorDetails {
                    field: Some("token".to_string()),
                    message: "Your authentication token has expired.".to_string(),
                    suggestion: Some(
                        "Your session has timed out. Please login again to continue."
                            .to_string(),
                    ),
                    action: Some(ErrorAction {
                        action_type: "login".to_string(),
                        endpoint: Some("/api/v1/auth/login".to_string()),
                        method: Some("POST".to_string()),
                        data: Some(serde_json::json!({
                            "email": "your-email@example.com",
                            "password": "your-password"
                        })),
                    }),
                })
            }
            Self::Unauthorized(msg) if msg.contains("Authentication failed") => {
                Some(ErrorDetails {
                    field: Some("token".to_string()),
                    message: msg.clone(),
                    suggestion: Some(
                        "Your token is invalid or malformed. Please login again."
                            .to_string(),
                    ),
                    action: Some(ErrorAction {
                        action_type: "login".to_string(),
                        endpoint: Some("/api/v1/auth/login".to_string()),
                        method: Some("POST".to_string()),
                        data: None,
                    }),
                })
            }
            Self::Orcher(OrcherError::Validation { field, message, .. }) => Some(ErrorDetails {
                field: Some(field.clone()),
                message: message.clone(),
                suggestion: Some(format!("Invalid value for field '{field}'")),
                action: None,
            }),
            Self::JsonRejection(msg) => Some(ErrorDetails {
                field: None,
                message: msg.clone(),
                suggestion: Some("Please ensure your JSON is properly formatted".to_string()),
                action: None,
            }),
            _ => None,
        }
    }
}

/// Detailed error information for client-facing responses
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorDetails {
    /// The field that caused the error (if applicable)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub field: Option<String>,
    /// Detailed error message
    pub message: String,
    /// Suggestion for fixing the error
    #[serde(skip_serializing_if = "Option::is_none")]
    pub suggestion: Option<String>,
    /// Actionable information for the client (e.g., API endpoint to call, email to resend to)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub action: Option<ErrorAction>,
}

/// Actionable error information that clients can use to help users fix the problem
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorAction {
    /// Type of action (e.g., "`resend_verification`", "`reset_password`")
    #[serde(rename = "type")]
    pub action_type: String,
    /// Endpoint to call for this action
    #[serde(skip_serializing_if = "Option::is_none")]
    pub endpoint: Option<String>,
    /// HTTP method for the endpoint
    #[serde(skip_serializing_if = "Option::is_none")]
    pub method: Option<String>,
    /// Additional data needed for the action
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<serde_json::Value>,
}

/// Standardized error response format
#[derive(Debug, Serialize, Deserialize)]
pub struct ErrorResponse {
    /// Error information
    pub error: ErrorInfo,
}

/// Error information structure
#[derive(Debug, Serialize, Deserialize)]
pub struct ErrorInfo {
    /// Error code (e.g., "`VALIDATION_ERROR`", "`NOT_FOUND`")
    pub code: String,
    /// HTTP status code
    pub status: u16,
    /// Human-readable error message
    pub message: String,
    /// Detailed error information (optional)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub details: Option<ErrorDetails>,
    /// Timestamp when error occurred
    pub timestamp: String,
    /// Request ID for tracing (optional)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
}

impl From<JsonRejection> for ServerError {
    fn from(rejection: JsonRejection) -> Self {
        let message = match rejection {
            JsonRejection::JsonDataError(e) => {
                format!("Invalid JSON data: {e}")
            }
            JsonRejection::JsonSyntaxError(e) => {
                format!("JSON syntax error: {e}")
            }
            JsonRejection::MissingJsonContentType(e) => {
                format!("Missing or invalid Content-Type header: {e}")
            }
            _ => format!("Failed to parse JSON: {rejection}"),
        };
        ServerError::JsonRejection(message)
    }
}

/// Implement `IntoResponse` for `ServerError` to allow handlers to return Result<T, `ServerError`>
impl IntoResponse for ServerError {
    fn into_response(self) -> Response {
        let status = self.status_code();

        // Log the full error server-side, but never leak internal/DB error
        // text to clients on 5xx — those messages carry SQL, schema, and
        // connection details. Client-facing 4xx messages are ours and safe.
        if status.is_server_error() {
            tracing::error!(error = %self, status = status.as_u16(), "request failed");
        }

        let code = self.error_code().to_string();
        let message = self.client_message();
        let details = self.details();

        let error_response = ErrorResponse {
            error: ErrorInfo {
                code,
                status: status.as_u16(),
                message,
                details,
                timestamp: Timestamptz::now().to_rfc3339(),
                request_id: None, // TODO: Extract from request context
            },
        };

        (status, Json(error_response)).into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// An engine refusal keeps its meaning and its explanation over REST.
    #[test]
    fn an_engine_refusal_becomes_the_matching_http_error() {
        for (code, status) in [
            (tonic::Code::NotFound, StatusCode::NOT_FOUND),
            (tonic::Code::InvalidArgument, StatusCode::BAD_REQUEST),
            (tonic::Code::AlreadyExists, StatusCode::CONFLICT),
            (tonic::Code::PermissionDenied, StatusCode::FORBIDDEN),
            (tonic::Code::Unauthenticated, StatusCode::UNAUTHORIZED),
            (tonic::Code::Unavailable, StatusCode::SERVICE_UNAVAILABLE),
            (tonic::Code::Internal, StatusCode::BAD_GATEWAY),
        ] {
            let error = ServerError::from_status(tonic::Status::new(code, "the engine's reason"));
            assert_eq!(error.status_code(), status, "{code:?}");
            if status.is_client_error() {
                assert!(error.to_string().contains("the engine's reason"), "{error}");
            }
        }
    }
}
