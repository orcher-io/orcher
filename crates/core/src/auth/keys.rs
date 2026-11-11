//! Issuing, listing and revoking the API keys the engine stores.
//!
//! [`ApiKeyAuthenticator`](super::ApiKeyAuthenticator) reads the `api_keys`
//! table; this writes it. A key is `orch_` and 32 random characters; the
//! table holds its SHA-256 and its first characters, never the key, so the
//! key is seen once, when it is issued.
//!
//! Keys are managed within one organisation, or within none on a
//! single-tenant deployment: a caller sees and revokes only the keys of the
//! organisation it acts for. Keys that may act for any organisation are
//! platform credentials, issued by whoever runs the deployment, and are
//! neither listed nor revocable here.
//!
//! A revoked key is refused at once on every node: revoking publishes the
//! key's id on [`REVOKED_CHANNEL`], and each engine forgets what it
//! remembered about the key ([`follow_revocations`]).

use super::{CachedAuthenticator, Scopes};
use chrono::{DateTime, Utc};
use serde::Serialize;
use std::sync::Arc;
use tracing::{info, warn};
use uuid::Uuid;

/// The Postgres notification channel a revoked key's id is published on.
pub const REVOKED_CHANNEL: &str = "orcher_api_key_revoked";

/// What every issued key starts with, so a key is recognisable as one (in a
/// log, or by a secret scanner) without being looked up.
pub const KEY_PREFIX: &str = "orch_";

/// Random characters after [`KEY_PREFIX`]: 32 base-36 characters, about 165
/// bits.
const RANDOM_CHARS: usize = 32;

/// Characters of a key kept in the table to tell keys apart in a listing.
const SHOWN_CHARS: usize = 12;

/// A key as listed: everything but the key itself.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, sqlx::FromRow)]
#[serde(rename_all = "camelCase")]
pub struct ApiKeyInfo {
    /// The key's id; its principal's subject is `key_<id>`.
    pub id: Uuid,
    /// What the key is for.
    pub name: String,
    /// More about what the key is for.
    pub description: Option<String>,
    /// The key's first characters, to tell it apart from others.
    pub key_prefix: String,
    /// The organisation the key acts for; `None` on a single-tenant
    /// deployment.
    pub organization_id: Option<Uuid>,
    /// The scopes the key grants; `None` for a key issued before keys had
    /// scopes, which grants every scope.
    pub scopes: Option<Vec<String>>,
    /// Who issued the key; `None` for a key issued before this was recorded.
    pub created_by: Option<String>,
    /// When the key was issued.
    pub created_at: DateTime<Utc>,
    /// When the key stops being accepted; never, when `None`.
    pub expires_at: Option<DateTime<Utc>>,
    /// When the key was last accepted, roughly.
    pub last_used_at: Option<DateTime<Utc>>,
    /// When the key was revoked; a revoked key is refused.
    pub revoked_at: Option<DateTime<Utc>>,
    /// Who revoked the key.
    pub revoked_by: Option<String>,
}

/// A key just issued: the key itself, shown this once, and its listing.
#[derive(Debug, Clone)]
pub struct IssuedKey {
    /// The key itself. It is not stored, so this is the only time it is seen.
    pub key: String,
    /// The key as it is listed.
    pub info: ApiKeyInfo,
}

/// What to issue.
#[derive(Debug, Clone)]
pub struct NewKey {
    /// What the key is for.
    pub name: String,
    /// More about what the key is for.
    pub description: Option<String>,
    /// The organisation the key acts for; `None` on a single-tenant
    /// deployment.
    pub organization_id: Option<Uuid>,
    /// What the key may do.
    pub scopes: Scopes,
    /// When the key stops being accepted; never, when `None`.
    pub expires_at: Option<DateTime<Utc>>,
    /// Who asked for it: the issuing caller's subject.
    pub created_by: String,
}

const COLUMNS: &str = "id, name, description, key_prefix, organization_id, scopes, created_by, \
                       created_at, expires_at, last_used_at, revoked_at, revoked_by";

/// The `api_keys` table, as one organisation's keys.
#[derive(Clone)]
pub struct ApiKeyStore {
    pool: sqlx::PgPool,
}

impl ApiKeyStore {
    /// The keys in the database `pool` connects to.
    pub fn new(pool: sqlx::PgPool) -> Self {
        Self { pool }
    }

    /// A new random key, in the form the engine issues.
    pub fn generate() -> String {
        use rand::Rng;
        const ALPHABET: &[u8] = b"0123456789abcdefghijklmnopqrstuvwxyz";
        let mut rng = rand::rngs::OsRng;
        let random: String = (0..RANDOM_CHARS)
            .map(|_| ALPHABET[rng.gen_range(0..ALPHABET.len())] as char)
            .collect();
        format!("{KEY_PREFIX}{random}")
    }

    /// Issue a key.
    pub async fn issue(&self, new: NewKey) -> Result<IssuedKey, sqlx::Error> {
        let key = Self::generate();
        let info = sqlx::query_as::<_, ApiKeyInfo>(&format!(
            "INSERT INTO api_keys (name, description, organization_id, key_prefix, key_hash, \
                                   scopes, created_by, expires_at) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8) \
             RETURNING {COLUMNS}"
        ))
        .bind(&new.name)
        .bind(&new.description)
        .bind(new.organization_id)
        .bind(&key[..SHOWN_CHARS])
        .bind(super::ApiKeyAuthenticator::hash_key(&key))
        .bind(new.scopes.names())
        .bind(&new.created_by)
        .bind(new.expires_at)
        .fetch_one(&self.pool)
        .await?;
        info!(key = %info.id, prefix = %info.key_prefix, organization = ?info.organization_id,
              created_by = %new.created_by, "API key issued");
        Ok(IssuedKey { key, info })
    }

    /// The organisation's keys, newest first, revoked ones included.
    pub async fn list(
        &self,
        organization_id: Option<Uuid>,
    ) -> Result<Vec<ApiKeyInfo>, sqlx::Error> {
        sqlx::query_as::<_, ApiKeyInfo>(&format!(
            "SELECT {COLUMNS} FROM api_keys \
             WHERE organization_id IS NOT DISTINCT FROM $1 AND NOT can_delegate_organization \
             ORDER BY created_at DESC, id"
        ))
        .bind(organization_id)
        .fetch_all(&self.pool)
        .await
    }

    /// One of the organisation's keys.
    pub async fn get(
        &self,
        organization_id: Option<Uuid>,
        id: Uuid,
    ) -> Result<Option<ApiKeyInfo>, sqlx::Error> {
        sqlx::query_as::<_, ApiKeyInfo>(&format!(
            "SELECT {COLUMNS} FROM api_keys \
             WHERE id = $1 AND organization_id IS NOT DISTINCT FROM $2 \
               AND NOT can_delegate_organization"
        ))
        .bind(id)
        .bind(organization_id)
        .fetch_optional(&self.pool)
        .await
    }

    /// Revoke one of the organisation's keys, and tell every engine to stop
    /// accepting it. Revoking a revoked key changes nothing and answers the
    /// key as it is; `None` if the organisation has no such key.
    pub async fn revoke(
        &self,
        organization_id: Option<Uuid>,
        id: Uuid,
        revoked_by: &str,
    ) -> Result<Option<ApiKeyInfo>, sqlx::Error> {
        let mut tx = self.pool.begin().await?;
        let revoked = sqlx::query_as::<_, ApiKeyInfo>(&format!(
            "UPDATE api_keys SET revoked_at = NOW(), revoked_by = $3 \
             WHERE id = $1 AND organization_id IS NOT DISTINCT FROM $2 \
               AND NOT can_delegate_organization AND revoked_at IS NULL \
             RETURNING {COLUMNS}"
        ))
        .bind(id)
        .bind(organization_id)
        .bind(revoked_by)
        .fetch_optional(&mut *tx)
        .await?;
        if revoked.is_none() {
            tx.rollback().await?;
            return self.get(organization_id, id).await;
        }
        // Delivered when the transaction commits, so no engine forgets the
        // key before the row says it is revoked.
        sqlx::query("SELECT pg_notify($1, $2)")
            .bind(REVOKED_CHANNEL)
            .bind(id.to_string())
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        info!(key = %id, revoked_by, "API key revoked");
        Ok(revoked)
    }
}

/// Forget what `cache` remembers about each key revoked anywhere, as the
/// revocation is published. Runs until the process ends, reconnecting if the
/// connection drops; on reconnecting it forgets every key, since a
/// revocation may have been missed while it was away.
pub async fn follow_revocations(pool: sqlx::PgPool, cache: Arc<CachedAuthenticator>) {
    use sqlx::postgres::PgListener;
    let mut first = true;
    loop {
        let mut listener = match PgListener::connect_with(&pool).await {
            Ok(listener) => listener,
            Err(e) => {
                warn!(error = %e, "Cannot follow API key revocations; retrying");
                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                continue;
            }
        };
        if let Err(e) = listener.listen(REVOKED_CHANNEL).await {
            warn!(error = %e, "Cannot follow API key revocations; retrying");
            tokio::time::sleep(std::time::Duration::from_secs(5)).await;
            continue;
        }
        if !first {
            cache.invalidate_all();
        }
        first = false;
        loop {
            match listener.recv().await {
                Ok(notification) => match notification.payload().parse::<Uuid>() {
                    Ok(id) => cache.invalidate_subject(&format!("key_{id}")),
                    Err(_) => warn!(
                        payload = notification.payload(),
                        "Unreadable API key revocation"
                    ),
                },
                Err(e) => {
                    warn!(error = %e, "Lost the API key revocation feed; reconnecting");
                    break;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_key_is_recognisable_and_random() {
        let (a, b) = (ApiKeyStore::generate(), ApiKeyStore::generate());
        assert_ne!(a, b);
        for key in [a, b] {
            assert!(key.starts_with(KEY_PREFIX), "{key}");
            assert_eq!(key.len(), KEY_PREFIX.len() + RANDOM_CHARS);
            assert!(key[KEY_PREFIX.len()..]
                .bytes()
                .all(|c| c.is_ascii_digit() || c.is_ascii_lowercase()));
        }
    }

    /// Issue, list, revoke against a real database: a key is listed only to
    /// its own organisation, revoking it is idempotent, and a revoked key is
    /// refused.
    #[tokio::test]
    #[ignore] // Requires PostgreSQL
    async fn keys_are_issued_listed_and_revoked_within_their_organisation() {
        use super::super::{ApiKeyAuthenticator, Authenticator, Credential};
        let url = std::env::var("TEST_DATABASE_URL").unwrap_or_else(|_| {
            "postgresql://postgres:postgres@localhost:5432/orcher_test".to_string()
        });
        let pool = sqlx::PgPool::connect(&url).await.unwrap();
        let store = ApiKeyStore::new(pool.clone());
        let (scopes, _) = Scopes::parse(["workflows:read"]);
        let issued = store
            .issue(NewKey {
                name: "keys test".into(),
                description: None,
                organization_id: None,
                scopes,
                expires_at: None,
                created_by: "test".into(),
            })
            .await
            .unwrap();
        assert_eq!(issued.info.key_prefix, issued.key[..SHOWN_CHARS]);
        assert_eq!(issued.info.scopes, Some(vec!["workflows:read".to_string()]));

        let keys = ApiKeyAuthenticator::new(pool.clone());
        let principal = keys
            .authenticate(Credential::Bearer(&issued.key))
            .await
            .unwrap();
        assert_eq!(principal.subject, format!("key_{}", issued.info.id));

        assert!(store
            .list(None)
            .await
            .unwrap()
            .iter()
            .any(|k| k.id == issued.info.id));
        let elsewhere = Some(Uuid::new_v4());
        assert!(store
            .get(elsewhere, issued.info.id)
            .await
            .unwrap()
            .is_none());
        assert!(store
            .revoke(elsewhere, issued.info.id, "test")
            .await
            .unwrap()
            .is_none());

        let revoked = store
            .revoke(None, issued.info.id, "test")
            .await
            .unwrap()
            .unwrap();
        assert!(revoked.revoked_at.is_some());
        let again = store
            .revoke(None, issued.info.id, "other")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(again.revoked_at, revoked.revoked_at);
        assert_eq!(again.revoked_by.as_deref(), Some("test"));
        assert!(keys
            .authenticate(Credential::Bearer(&issued.key))
            .await
            .is_err());

        sqlx::query("DELETE FROM api_keys WHERE id = $1")
            .bind(issued.info.id)
            .execute(&pool)
            .await
            .unwrap();
    }
}
