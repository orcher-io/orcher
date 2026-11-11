//! Tokens an OpenID Connect identity provider issued.
//!
//! The engine keeps no accounts: a deployment that has an identity provider
//! (Keycloak, Okta, Auth0, Azure AD, Google, ...) points the engine at it, and
//! a caller presents the access token the provider gave it. The engine checks
//! the token against the keys the provider publishes, and that it was issued
//! by that provider, for this engine, and is still current; the caller is the
//! token's subject, acting for the organisation a claim of the token names.

use super::{AuthError, Authenticator, Credential, Principal, Scopes};
use async_trait::async_trait;
use jsonwebtoken::jwk::JwkSet;
use jsonwebtoken::{decode, decode_header, Algorithm, DecodingKey, Validation};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
#[cfg(test)]
use std::time::SystemTime;
use std::time::{Duration, Instant, UNIX_EPOCH};
use tokio::sync::RwLock;
use tracing::{debug, warn};
use uuid::Uuid;

/// Which identity provider to trust, and how to read its tokens.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OidcConfig {
    /// The provider's issuer URL, exactly as its tokens' `iss` claim and its
    /// discovery document state it, e.g. `https://idp.example.com/realms/acme`.
    pub issuer: String,
    /// The audience a token must be issued for (its `aud` claim must contain
    /// it): the name this engine is registered under at the provider.
    pub audience: String,
    /// The claim naming the organisation the caller acts for, as an
    /// organisation id (a UUID, optionally `org_`-prefixed). Unset, callers
    /// belong to no organisation: a single-tenant deployment.
    #[serde(default)]
    pub organization_claim: Option<String>,
    /// The claim listing what the caller may do, as scope names
    /// (`workflows:read`, `workers:run`, ... or `*`): a space-separated string,
    /// as OAuth's `scope` claim is, or an array. A token without it may do
    /// nothing. Unset, every token may do everything.
    #[serde(default)]
    pub scopes_claim: Option<String>,
    /// How much clock difference with the provider to allow when checking a
    /// token's expiry and not-before times, in seconds.
    #[serde(default = "default_leeway_secs")]
    pub leeway_secs: u64,
}

fn default_leeway_secs() -> u64 {
    60
}

/// Asymmetric algorithms only: a token signed with a shared secret could be
/// forged by anyone who holds the secret, and the engine is not one of the
/// parties that should.
const ALGORITHMS: [Algorithm; 9] = [
    Algorithm::RS256,
    Algorithm::RS384,
    Algorithm::RS512,
    Algorithm::PS256,
    Algorithm::PS384,
    Algorithm::PS512,
    Algorithm::ES256,
    Algorithm::ES384,
    Algorithm::EdDSA,
];

/// The least time between two fetches of the provider's keys: an unknown key
/// id fetches them again (the provider rotated its keys), but a stream of
/// tokens naming keys that do not exist must not become a stream of fetches.
const MIN_REFETCH: Duration = Duration::from_secs(30);

/// The provider's published keys, and when they were fetched.
struct Keys {
    set: JwkSet,
    fetched_at: Instant,
}

/// Accepts access tokens the configured provider issued for this engine.
///
/// The provider's discovery document and keys are fetched on first use, not
/// at startup, so a provider that is briefly unreachable when the engine
/// starts does not stop it; tokens are refused, as unauthenticated, until the
/// keys can be fetched.
pub struct OidcAuthenticator {
    config: OidcConfig,
    http: reqwest::Client,
    keys: RwLock<Option<Keys>>,
}

impl OidcAuthenticator {
    /// Trust the provider `config` names.
    pub fn new(config: OidcConfig) -> Self {
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .unwrap_or_else(|_| reqwest::Client::new());
        Self {
            config,
            http,
            keys: RwLock::new(None),
        }
    }

    /// The provider's keys, fetched now: its discovery document, which must
    /// name the configured issuer, then the key set it points to.
    async fn fetch_keys(&self) -> Result<JwkSet, String> {
        #[derive(Deserialize)]
        struct Discovery {
            issuer: String,
            jwks_uri: String,
        }
        let url = format!(
            "{}/.well-known/openid-configuration",
            self.config.issuer.trim_end_matches('/')
        );
        let discovery: Discovery = self
            .http
            .get(&url)
            .send()
            .await
            .and_then(reqwest::Response::error_for_status)
            .map_err(|e| format!("fetching {url}: {e}"))?
            .json()
            .await
            .map_err(|e| format!("reading {url}: {e}"))?;
        if discovery.issuer != self.config.issuer {
            return Err(format!(
                "{url} names issuer {}, not the configured {}",
                discovery.issuer, self.config.issuer
            ));
        }
        self.http
            .get(&discovery.jwks_uri)
            .send()
            .await
            .and_then(reqwest::Response::error_for_status)
            .map_err(|e| format!("fetching {}: {e}", discovery.jwks_uri))?
            .json()
            .await
            .map_err(|e| format!("reading {}: {e}", discovery.jwks_uri))
    }

    /// The key a token signed with `kid` (or with no key id) verifies with,
    /// fetching the provider's keys when none are held or `kid` is not among
    /// them and they were not fetched within [`MIN_REFETCH`].
    async fn key_for(&self, kid: Option<&str>) -> Result<DecodingKey, AuthError> {
        if let Some(key) = self.held_key(kid).await? {
            return Ok(key);
        }
        {
            let mut keys = self.keys.write().await;
            let fetched_recently = keys
                .as_ref()
                .is_some_and(|k| k.fetched_at.elapsed() < MIN_REFETCH);
            if !fetched_recently {
                match self.fetch_keys().await {
                    Ok(set) => {
                        debug!(
                            keys = set.keys.len(),
                            "Fetched the identity provider's keys"
                        );
                        *keys = Some(Keys {
                            set,
                            fetched_at: Instant::now(),
                        });
                    }
                    Err(e) => {
                        warn!(error = %e, "Could not fetch the identity provider's keys");
                        if keys.is_none() {
                            return Err(AuthError::Unauthenticated(
                                "the identity provider's keys are unavailable".to_string(),
                            ));
                        }
                    }
                }
            }
        }
        self.held_key(kid)
            .await?
            .ok_or_else(|| AuthError::Unauthenticated("token signed with an unknown key".into()))
    }

    /// The held key for `kid`: the one with that id, or, for a token with no
    /// key id, the only key when there is exactly one.
    async fn held_key(&self, kid: Option<&str>) -> Result<Option<DecodingKey>, AuthError> {
        let keys = self.keys.read().await;
        let Some(keys) = keys.as_ref() else {
            return Ok(None);
        };
        let jwk = match kid {
            Some(kid) => keys.set.find(kid),
            None if keys.set.keys.len() == 1 => keys.set.keys.first(),
            None => None,
        };
        jwk.map(|jwk| {
            DecodingKey::from_jwk(jwk)
                .map_err(|e| AuthError::Unauthenticated(format!("unusable provider key: {e}")))
        })
        .transpose()
    }

    /// The caller a verified token's claims describe.
    fn principal(&self, claims: &serde_json::Value) -> Result<Principal, AuthError> {
        let subject = claims
            .get("sub")
            .and_then(|s| s.as_str())
            .filter(|s| !s.is_empty())
            .ok_or_else(|| AuthError::Unauthenticated("token has no subject".into()))?;
        let name = ["preferred_username", "email", "client_id", "azp"]
            .iter()
            .find_map(|claim| claims.get(*claim).and_then(|v| v.as_str()))
            .unwrap_or(subject);
        let organization_id = match &self.config.organization_claim {
            None => None,
            Some(claim) => match claims.get(claim) {
                None | Some(serde_json::Value::Null) => None,
                Some(value) => Some(organization_of(value).ok_or_else(|| {
                    AuthError::Unauthenticated(format!(
                        "token's {claim} claim is not an organization id"
                    ))
                })?),
            },
        };
        let scopes = match &self.config.scopes_claim {
            None => Scopes::all(),
            Some(claim) => {
                let names: Vec<&str> = match claims.get(claim) {
                    Some(serde_json::Value::String(s)) => s.split_whitespace().collect(),
                    Some(serde_json::Value::Array(items)) => {
                        items.iter().filter_map(serde_json::Value::as_str).collect()
                    }
                    _ => Vec::new(),
                };
                Scopes::parse(names).0
            }
        };
        let valid_until = claims
            .get("exp")
            .and_then(serde_json::Value::as_u64)
            .map(|exp| UNIX_EPOCH + Duration::from_secs(exp));
        Ok(Principal {
            subject: subject.to_string(),
            name: name.to_string(),
            organization_id,
            scopes,
            valid_until,
            ..Principal::anonymous()
        })
    }
}

/// An organisation id as a claim carries it: a UUID, optionally prefixed
/// `org_`, as a string (or a one-element array of one, as some providers emit
/// a user attribute).
fn organization_of(value: &serde_json::Value) -> Option<Uuid> {
    let text = match value {
        serde_json::Value::String(s) => s.as_str(),
        serde_json::Value::Array(items) if items.len() == 1 => items[0].as_str()?,
        _ => return None,
    };
    Uuid::parse_str(text.strip_prefix("org_").unwrap_or(text)).ok()
}

#[async_trait]
impl Authenticator for OidcAuthenticator {
    async fn authenticate(&self, credential: Credential<'_>) -> Result<Principal, AuthError> {
        let Credential::Bearer(token) = credential else {
            return Err(AuthError::Unauthenticated("no token".to_string()));
        };
        let header = decode_header(token)
            .map_err(|e| AuthError::Unauthenticated(format!("not a token: {e}")))?;
        if !ALGORITHMS.contains(&header.alg) {
            return Err(AuthError::Unauthenticated(format!(
                "token signed with {:?}, which is not accepted",
                header.alg
            )));
        }
        let key = self.key_for(header.kid.as_deref()).await?;
        let mut validation = Validation::new(header.alg);
        validation.set_issuer(&[&self.config.issuer]);
        validation.set_audience(&[&self.config.audience]);
        validation.leeway = self.config.leeway_secs;
        validation.validate_nbf = true;
        validation.set_required_spec_claims(&["exp", "iss", "aud", "sub"]);
        let claims = decode::<serde_json::Value>(token, &key, &validation)
            .map_err(|e| AuthError::Unauthenticated(format!("token refused: {e}")))?
            .claims;
        self.principal(&claims)
    }
}

/// Routes a bearer credential by its shape: a JSON Web Token (three
/// dot-separated parts) to `tokens`, anything else to `keys`. A deployment
/// that issues both API keys (for workers) and provider tokens (for people)
/// accepts each where it belongs.
pub struct KeyOrToken {
    keys: Arc<dyn Authenticator>,
    tokens: Arc<dyn Authenticator>,
}

impl KeyOrToken {
    /// API keys checked by `keys`, tokens by `tokens`.
    pub fn new(keys: Arc<dyn Authenticator>, tokens: Arc<dyn Authenticator>) -> Self {
        Self { keys, tokens }
    }
}

/// Whether `credential` has the shape of a JSON Web Token.
fn looks_like_jwt(credential: &str) -> bool {
    let parts: Vec<&str> = credential.split('.').collect();
    parts.len() == 3
        && parts.iter().take(2).all(|part| {
            !part.is_empty()
                && part
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
        })
}

#[async_trait]
impl Authenticator for KeyOrToken {
    async fn authenticate(&self, credential: Credential<'_>) -> Result<Principal, AuthError> {
        match credential {
            Credential::Bearer(token) if looks_like_jwt(token) => {
                self.tokens.authenticate(credential).await
            }
            _ => self.keys.authenticate(credential).await,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// An unsigned token of the given header and claims: the shape of a JWT,
    /// built here rather than written out.
    fn shaped_token(header: serde_json::Value, claims: serde_json::Value) -> String {
        use base64::Engine as _;
        let part = |value: serde_json::Value| {
            base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(value.to_string())
        };
        format!("{}.{}.c2lnbmF0dXJl", part(header), part(claims))
    }

    fn now_secs() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or_default()
    }

    #[test]
    fn an_organisation_claim_is_read_as_its_id() {
        let id = Uuid::new_v4();
        assert_eq!(
            organization_of(&serde_json::json!(id.to_string())),
            Some(id)
        );
        assert_eq!(
            organization_of(&serde_json::json!(format!("org_{id}"))),
            Some(id)
        );
        assert_eq!(
            organization_of(&serde_json::json!([id.to_string()])),
            Some(id)
        );
        assert_eq!(organization_of(&serde_json::json!("acme")), None);
        assert_eq!(organization_of(&serde_json::json!(42)), None);
    }

    #[test]
    fn a_token_is_told_from_a_key_by_its_shape() {
        assert!(looks_like_jwt(&shaped_token(
            serde_json::json!({"alg": "RS256"}),
            serde_json::json!({"sub": "x"}),
        )));
        assert!(!looks_like_jwt("orch_live_0123456789abcdef"));
        assert!(!looks_like_jwt("key-alpha"));
        assert!(!looks_like_jwt("a.b"));
        assert!(!looks_like_jwt("a..c"));
        assert!(!looks_like_jwt("a b.c d.e"));
    }

    fn authenticator(organization_claim: Option<&str>) -> OidcAuthenticator {
        OidcAuthenticator::new(OidcConfig {
            issuer: "https://idp.example.com/realms/acme".into(),
            audience: "orcher".into(),
            organization_claim: organization_claim.map(str::to_string),
            scopes_claim: None,
            leeway_secs: 60,
        })
    }

    #[test]
    fn a_tokens_claims_describe_the_caller() {
        let org = Uuid::new_v4();
        let exp = now_secs() + 300;
        let principal = authenticator(Some("org_id"))
            .principal(&serde_json::json!({
                "sub": "u-1", "preferred_username": "ada", "org_id": org.to_string(), "exp": exp,
            }))
            .unwrap();
        assert_eq!(principal.subject, "u-1");
        assert_eq!(principal.name, "ada");
        assert_eq!(principal.organization_id, Some(org));
        assert_eq!(
            principal.valid_until,
            Some(UNIX_EPOCH + Duration::from_secs(exp))
        );
        assert!(!principal.can_act_for_any_organization());

        // Without the claim configured, no organisation whatever the token says.
        let principal = authenticator(None)
            .principal(&serde_json::json!({"sub": "u-1", "org_id": org.to_string()}))
            .unwrap();
        assert_eq!(principal.organization_id, None);

        // A configured claim that is not an organisation id is refused.
        assert!(authenticator(Some("org_id"))
            .principal(&serde_json::json!({"sub": "u-1", "org_id": "acme"}))
            .is_err());
        // And a token without a subject.
        assert!(authenticator(None)
            .principal(&serde_json::json!({"preferred_username": "ada"}))
            .is_err());
    }

    #[tokio::test]
    async fn a_token_signed_with_a_shared_secret_is_refused_before_any_key_is_fetched() {
        let token = jsonwebtoken::encode(
            &jsonwebtoken::Header::new(Algorithm::HS256),
            &serde_json::json!({"sub": "x", "exp": now_secs() + 60}),
            &jsonwebtoken::EncodingKey::from_secret(b"guessable"),
        )
        .unwrap();
        let refused = authenticator(None)
            .authenticate(Credential::Bearer(&token))
            .await
            .unwrap_err();
        assert!(refused.to_string().contains("HS256"), "{refused}");
    }

    #[tokio::test]
    async fn without_the_providers_keys_a_token_is_refused_as_unauthenticated() {
        // Nothing answers on this address.
        let unreachable = OidcAuthenticator::new(OidcConfig {
            issuer: "http://127.0.0.1:9/realms/none".into(),
            audience: "orcher".into(),
            organization_claim: None,
            scopes_claim: None,
            leeway_secs: 60,
        });
        let token = shaped_token(
            serde_json::json!({"alg": "RS256", "kid": "k1"}),
            serde_json::json!({"sub": "x"}),
        );
        assert!(matches!(
            unreachable.authenticate(Credential::Bearer(&token)).await,
            Err(AuthError::Unauthenticated(_))
        ));
    }

    #[test]
    fn a_tokens_scopes_come_from_its_claim_when_one_is_configured() {
        use crate::auth::Scope;
        let with_claim = OidcAuthenticator::new(OidcConfig {
            scopes_claim: Some("scope".into()),
            ..authenticator(None).config
        });
        let principal = with_claim
            .principal(&serde_json::json!({"sub": "u", "scope": "openid workflows:read profile"}))
            .unwrap();
        assert!(principal.scopes.allows(Scope::WorkflowsRead));
        assert!(!principal.scopes.allows(Scope::WorkflowsWrite));
        let principal = with_claim
            .principal(&serde_json::json!({"sub": "u", "scope": ["workers:run"]}))
            .unwrap();
        assert_eq!(principal.scopes.names(), ["workers:run"]);
        // No claim: nothing.
        let principal = with_claim
            .principal(&serde_json::json!({"sub": "u"}))
            .unwrap();
        assert_eq!(principal.scopes, Scopes::none());
        // Not configured: everything, as before scopes.
        let principal = authenticator(None)
            .principal(&serde_json::json!({"sub": "u"}))
            .unwrap();
        assert_eq!(principal.scopes, Scopes::all());
    }
}
