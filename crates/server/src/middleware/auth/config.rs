//! # Authentication Configuration
use orcher_common::error::{OrcherError, OrcherResult};
use serde::{Deserialize, Serialize};
use tracing::warn;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthConfig {
    pub jwt_secret: String,
    pub jwt_issuer: String,
    pub jwt_audience: Option<String>,
    pub token_expiration_secs: i64,
    pub enable_api_keys: bool,
    pub enable_oauth: bool,
    pub require_auth: bool,
    pub public_routes: Vec<String>,
    pub token_grace_period_secs: i64,
}

impl AuthConfig {
    /// Load auth config from environment, requiring `JWT_SECRET` to be set.
    ///
    /// In production (when `ORCHER_ENV=production`), this returns an error
    /// if `JWT_SECRET` is not set. In development/test, it falls back to
    /// a default secret with a warning.
    pub fn from_env() -> OrcherResult<Self> {
        let is_production = std::env::var("ORCHER_ENV")
            .map(|v| v.eq_ignore_ascii_case("production"))
            .unwrap_or(false);

        let jwt_secret = match std::env::var("JWT_SECRET") {
            Ok(secret) if !secret.is_empty() => secret,
            _ if is_production => {
                return Err(OrcherError::Internal {
                    message: "JWT_SECRET environment variable must be set in production"
                        .to_string(),
                    source: None,
                });
            }
            _ => {
                warn!("JWT_SECRET not set, using insecure default — do NOT use in production");
                "default-secret-change-in-production".to_string()
            }
        };

        Ok(Self {
            jwt_secret,
            ..Self::dev_defaults()
        })
    }

    pub fn new(jwt_secret: impl Into<String>, jwt_issuer: impl Into<String>) -> Self {
        Self {
            jwt_secret: jwt_secret.into(),
            jwt_issuer: jwt_issuer.into(),
            ..Self::dev_defaults()
        }
    }

    #[must_use]
    pub fn with_public_routes(mut self, routes: Vec<String>) -> Self {
        self.public_routes = routes;
        self
    }

    fn dev_defaults() -> Self {
        Self {
            jwt_secret: String::new(),
            jwt_issuer: "orcher".to_string(),
            jwt_audience: Some("orcher-api".to_string()),
            token_expiration_secs: 3600,
            enable_api_keys: true,
            enable_oauth: false,
            require_auth: true,
            public_routes: vec![
                "/health".to_string(),
                "/metrics".to_string(),
                "/api/v1/auth/login".to_string(),
                "/api/v1/auth/register".to_string(),
                "/api/v1/auth/refresh".to_string(),
                "/api/v1/auth/oauth/initiate".to_string(),
                "/api/v1/auth/oauth/callback".to_string(),
            ],
            token_grace_period_secs: 300,
        }
    }
}

impl Default for AuthConfig {
    fn default() -> Self {
        Self::from_env().unwrap_or_else(|e| {
            warn!(
                "Failed to load auth config from env: {}, using dev defaults",
                e
            );
            let mut defaults = Self::dev_defaults();
            defaults.jwt_secret = "default-secret-change-in-production".to_string();
            defaults
        })
    }
}
