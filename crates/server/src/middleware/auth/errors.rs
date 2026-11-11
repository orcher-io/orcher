//! # Authentication Errors
use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
};

#[derive(Debug, thiserror::Error)]
pub enum AuthError {
    #[error("Missing authentication credentials")]
    MissingCredentials,
    #[error("Invalid token: {0}")]
    InvalidToken(String),
    #[error("Expired token")]
    ExpiredToken,
    #[error("Invalid API key")]
    InvalidApiKey,
    #[error("Insufficient permissions")]
    InsufficientPermissions,
    #[error("Token revoked")]
    TokenRevoked,
    #[error("Internal authentication error: {0}")]
    Internal(String),
}

impl IntoResponse for AuthError {
    fn into_response(self) -> Response {
        let status = match self {
            AuthError::InsufficientPermissions => StatusCode::FORBIDDEN,
            _ => StatusCode::UNAUTHORIZED,
        };
        let body = serde_json::json!({
            "error": {
                "code": status.as_u16(),
                "message": self.to_string(),
                "type": "authentication_error"
            }
        });
        (status, axum::Json(body)).into_response()
    }
}
