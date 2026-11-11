//! API keys the engine stores, in the `api_keys` table.

use super::{AuthError, Authenticator, Credential, Principal, Scopes};
use async_trait::async_trait;
use orcher_common::error::OrcherError;
use tracing::warn;

/// Validates API keys against the `api_keys` table.
///
/// Keys are stored as the hex SHA-256 of the key itself, which is what the
/// issuing side writes; the plain key is never stored and never logged. A key
/// resolves to an identity carrying the organisation it was issued in, so
/// every request it authenticates is scoped to that organisation from the
/// edge in. The identity has no namespace of its own — an organisation holds
/// many — and every task queue; finer grants are a later concern.
pub struct ApiKeyAuthenticator {
    pool: sqlx::PgPool,
}

impl ApiKeyAuthenticator {
    /// Validate against the given database.
    pub fn new(pool: sqlx::PgPool) -> Self {
        Self { pool }
    }

    /// The stored form of a key: lower-case hex of its SHA-256.
    pub fn hash_key(api_key: &str) -> String {
        use sha2::{Digest, Sha256};
        hex::encode(Sha256::digest(api_key.as_bytes()))
    }
}

impl ApiKeyAuthenticator {
    /// The caller `api_key` belongs to, or why it is refused.
    async fn validate(&self, api_key: &str) -> Result<Principal, OrcherError> {
        let key_hash = Self::hash_key(api_key);

        // Expiry is decided by the database clock, the same one that stamped
        // `expires_at`, rather than by comparing timestamps across machines.
        #[allow(clippy::type_complexity)]
        let row: Option<(
            uuid::Uuid,
            String,
            Option<uuid::Uuid>,
            bool,
            bool,
            bool,
            Option<Vec<String>>,
        )> = sqlx::query_as(
            r#"SELECT id, name, organization_id, can_delegate_organization,
                      is_active AND revoked_at IS NULL,
                      (expires_at IS NOT NULL AND expires_at < NOW()) AS expired,
                      scopes
               FROM api_keys
               WHERE key_hash = $1"#,
        )
        .bind(&key_hash)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| OrcherError::authentication(format!("API key lookup failed: {e}")))?;

        // One message for every way a key can be bad. Telling a caller which
        // it was (unknown, revoked, expired) tells an attacker which keys
        // exist.
        let (id, name, organization_id, can_delegate, scopes) = match row {
            Some((id, name, organization_id, can_delegate, true, false, scopes)) => {
                (id, name, organization_id, can_delegate, scopes)
            }
            _ => return Err(OrcherError::authentication("invalid API key")),
        };

        // What a key without an organisation may do is the deployment's
        // tenancy's to decide (`organization_for`), not this lookup's.

        // Best effort, off the request path: a stale `last_used_at` is a
        // reporting inaccuracy, not an authentication failure.
        let pool = self.pool.clone();
        tokio::spawn(async move {
            let _ = sqlx::query("UPDATE api_keys SET last_used_at = NOW() WHERE id = $1")
                .bind(id)
                .execute(&pool)
                .await;
        });

        Ok(Principal {
            subject: format!("key_{id}"),
            name,
            organization_id,
            // An organisation holds many namespaces, and the key may reach
            // every task queue; finer grants are a later concern.
            namespace: None,
            task_queues: Vec::new(),
            // What the key was issued with; a key issued before scopes may do
            // everything it could then.
            scopes: match scopes {
                None => Scopes::all(),
                Some(names) => {
                    let (scopes, unknown) = Scopes::parse(names.iter().map(String::as_str));
                    if !unknown.is_empty() {
                        warn!(key = %id, ?unknown, "API key names scopes this engine does not know; they grant nothing");
                    }
                    scopes
                }
            },
            // Granted by the key row, never by default: a credential with it
            // can act as every tenant.
            can_act_for_any_organization: can_delegate,
            // As before: a key's own expiry is checked when it is looked up,
            // and a cached one lives for the cache's TTL.
            valid_until: None,
        })
    }
}

#[async_trait]
impl Authenticator for ApiKeyAuthenticator {
    async fn authenticate(&self, credential: Credential<'_>) -> Result<Principal, AuthError> {
        let Credential::Bearer(api_key) = credential else {
            return Err(AuthError::Unauthenticated("no API key".to_string()));
        };
        self.validate(api_key).await.map_err(|e| match e {
            // A valid key that may not act (one with no organisation on a
            // multi-tenant deployment) is refused as not permitted, not as
            // unknown: the caller is who it says, it just may not do this.
            OrcherError::Authorization { .. } => {
                warn!(error = %e, "API key refused");
                AuthError::Forbidden(e.to_string())
            }
            _ => {
                warn!(error = %e, "API key validation failed");
                AuthError::Unauthenticated(e.to_string())
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A key carries its organisation and whether it may act for any, and
    /// nothing else is looked up: no organisation row need exist, and what a
    /// key without an organisation may do is the declared tenancy's to decide
    /// (`organization_for`). Unknown keys and missing credentials are refused.
    #[tokio::test]
    #[ignore] // Requires PostgreSQL
    async fn a_key_is_its_organisation_and_its_right_to_delegate() {
        let url = std::env::var("TEST_DATABASE_URL").unwrap_or_else(|_| {
            "postgresql://postgres:postgres@localhost:5432/orcher_test".to_string()
        });
        let pool = sqlx::PgPool::connect(&url)
            .await
            .expect("connect to the test database");
        crate_migrations(&pool).await;
        // An organisation with no row anywhere: an opaque tenancy key.
        let org = uuid::Uuid::new_v4();
        let tag = uuid::Uuid::new_v4().simple().to_string();
        let key = |organization: Option<uuid::Uuid>, delegate: bool| {
            let pool = pool.clone();
            let tag = tag.clone();
            async move {
                let secret = format!("orch_test_{}", uuid::Uuid::new_v4());
                sqlx::query(
                    "INSERT INTO api_keys (organization_id, name, key_prefix, key_hash, \
                     can_delegate_organization) VALUES ($1, $2, 'orch_test', $3, $4)",
                )
                .bind(organization)
                .bind(format!("auth test {tag}"))
                .bind(ApiKeyAuthenticator::hash_key(&secret))
                .bind(delegate)
                .execute(&pool)
                .await
                .unwrap();
                secret
            }
        };
        let (ownerless, bound, delegating) = (
            key(None, false).await,
            key(Some(org), false).await,
            key(None, true).await,
        );

        let keys = ApiKeyAuthenticator::new(pool.clone());
        let unscoped = keys
            .authenticate(Credential::Bearer(&ownerless))
            .await
            .unwrap();
        assert_eq!(unscoped.organization_id, None);
        assert!(!unscoped.can_act_for_any_organization());
        assert_eq!(
            keys.authenticate(Credential::Bearer(&bound))
                .await
                .unwrap()
                .organization_id,
            Some(org)
        );
        assert!(keys
            .authenticate(Credential::Bearer(&delegating))
            .await
            .unwrap()
            .can_act_for_any_organization());
        assert!(matches!(
            keys.authenticate(Credential::Bearer("orch_unknown")).await,
            Err(AuthError::Unauthenticated(_))
        ));
        assert!(matches!(
            keys.authenticate(Credential::None).await,
            Err(AuthError::Unauthenticated(_))
        ));

        sqlx::query("DELETE FROM api_keys WHERE name = $1")
            .bind(format!("auth test {tag}"))
            .execute(&pool)
            .await
            .unwrap();
    }

    async fn crate_migrations(pool: &sqlx::PgPool) {
        orcher_storage::setup::run_migrations(pool)
            .await
            .expect("apply migrations");
    }

    #[test]
    fn a_key_hashes_the_way_the_issuer_stores_it() {
        // Lower-case hex SHA-256, no salt: the issuing side computes exactly
        // this, so a key it stored is a key this finds. The vector is the
        // published SHA-256 of "abc".
        assert_eq!(
            ApiKeyAuthenticator::hash_key("abc"),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }
}
