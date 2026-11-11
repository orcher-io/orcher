//! # Authentication Context
use orcher_common::types::identifiers::{SessionId, UserId};
use orcher_common::types::time::Timestamptz;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

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
    #[must_use]
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

    #[must_use]
    pub fn with_scopes(mut self, scopes: Vec<String>) -> Self {
        self.scopes = scopes;
        self
    }

    #[must_use]
    pub fn has_scope(&self, scope: &str) -> bool {
        self.scopes.iter().any(|s| s == scope)
    }

    #[must_use]
    pub fn is_expired(&self) -> bool {
        self.expires_at < Timestamptz::now()
    }

    #[must_use]
    pub fn is_near_expiry(&self) -> bool {
        self.expires_at - Timestamptz::now() < jiff::SignedDuration::from_mins(5)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum AuthMethod {
    Jwt,
    ApiKey,
    OAuth2,
    Session,
    Basic,
    MultiFactor,
}
