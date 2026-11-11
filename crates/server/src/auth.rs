//! Authentication provider trait.
//!
//! Defines the interface between the core server and authentication implementations.
//! The open source server ships with [`NoOpAuthProvider`] (all requests pass through).
//! The cloud gateway plugs in a real JWT/OAuth implementation.

use async_trait::async_trait;
use orcher_common::types::identifiers::{SessionId, UserId};
use orcher_common::types::time::Timestamptz;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Result of authenticating a request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthenticatedUser {
    pub user_id: UserId,
    pub session_id: SessionId,
    pub scopes: Vec<String>,
    pub expires_at: Timestamptz,
    pub issued_at: Timestamptz,
    pub custom_claims: HashMap<String, serde_json::Value>,
    pub auth_method: AuthMethod,
}

impl AuthenticatedUser {
    /// Create an anonymous user (for open source / no-auth mode).
    pub fn anonymous() -> Self {
        Self {
            user_id: UserId::from_uuid(uuid::Uuid::nil()),
            session_id: SessionId::from_uuid(uuid::Uuid::nil()),
            scopes: vec!["*".to_string()],
            expires_at: Timestamptz::now() + jiff::SignedDuration::from_hours(24 * 365),
            issued_at: Timestamptz::now(),
            custom_claims: HashMap::new(),
            auth_method: AuthMethod::None,
        }
    }

    pub fn new(user_id: UserId, session_id: SessionId, auth_method: AuthMethod) -> Self {
        Self {
            user_id,
            session_id,
            scopes: Vec::new(),
            expires_at: Timestamptz::now() + jiff::SignedDuration::from_hours(1),
            issued_at: Timestamptz::now(),
            custom_claims: HashMap::new(),
            auth_method,
        }
    }

    pub fn with_scopes(mut self, scopes: Vec<String>) -> Self {
        self.scopes = scopes;
        self
    }

    pub fn has_scope(&self, scope: &str) -> bool {
        self.scopes.iter().any(|s| s == "*" || s == scope)
    }

    pub fn is_expired(&self) -> bool {
        self.expires_at < Timestamptz::now()
    }

    pub fn is_near_expiry(&self) -> bool {
        self.expires_at - Timestamptz::now() < jiff::SignedDuration::from_mins(5)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum AuthMethod {
    None,
    Jwt,
    ApiKey,
    OAuth2,
    Session,
    Basic,
}

/// Authentication error.
#[derive(Debug, thiserror::Error)]
pub enum AuthError {
    #[error("Missing authentication token")]
    MissingToken,

    #[error("Invalid token: {0}")]
    InvalidToken(String),

    #[error("Token expired")]
    TokenExpired,

    #[error("Internal error: {0}")]
    Internal(String),
}

/// Trait for authenticating requests.
///
/// Implementations validate a bearer token and return an [`AuthenticatedUser`].
/// The core server calls this from the authentication middleware on every
/// protected route.
///
/// # Open Source Default
///
/// [`NoOpAuthProvider`] allows all requests through with an anonymous identity.
/// This is suitable for single-tenant self-hosted deployments.
///
/// # Cloud Implementation
///
/// The cloud gateway provides a JWT-based implementation that validates tokens
/// against the session store and extracts user/org context.
#[async_trait]
pub trait AuthProvider: Send + Sync + 'static {
    /// Validate a bearer token and return the authenticated user.
    async fn authenticate(&self, token: &str) -> Result<AuthenticatedUser, AuthError>;
}

/// No-op auth provider — all requests pass through as anonymous.
///
/// Used by the open source server when no auth is configured.
pub struct NoOpAuthProvider;

#[async_trait]
impl AuthProvider for NoOpAuthProvider {
    async fn authenticate(&self, _token: &str) -> Result<AuthenticatedUser, AuthError> {
        Ok(AuthenticatedUser::anonymous())
    }
}
