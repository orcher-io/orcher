//! An [`Authenticator`] that remembers the callers it identified.

use super::{AuthError, Authenticator, Credential, Principal};
use async_trait::async_trait;
use moka::future::Cache;
use std::sync::Arc;
use std::time::Duration;
use tracing::debug;

/// How long, and how many, identified callers are remembered.
#[derive(Debug, Clone)]
pub struct AuthCacheConfig {
    /// How long an identified caller stays remembered.
    pub ttl: Duration,
    /// Most callers remembered at once.
    pub max_entries: u64,
}

impl Default for AuthCacheConfig {
    fn default() -> Self {
        Self {
            ttl: Duration::from_secs(300),
            max_entries: 10_000,
        }
    }
}

/// Asks `inner` about a bearer token once, and answers from memory until the
/// answer expires. Refusals are not remembered, so a key issued a moment ago
/// works at once; a revoked key keeps working until its entry expires (the
/// TTL), or until [`invalidate`](Self::invalidate) is called.
pub struct CachedAuthenticator {
    inner: Arc<dyn Authenticator>,
    cache: Cache<String, Principal>,
}

impl CachedAuthenticator {
    /// `inner`, remembered as the default configuration says.
    pub fn new(inner: Arc<dyn Authenticator>) -> Self {
        Self::with_config(inner, AuthCacheConfig::default())
    }

    /// `inner`, remembered as `config` says.
    pub fn with_config(inner: Arc<dyn Authenticator>, config: AuthCacheConfig) -> Self {
        let cache = Cache::builder()
            .max_capacity(config.max_entries)
            .time_to_live(config.ttl)
            .support_invalidation_closures()
            .build();
        Self { inner, cache }
    }

    /// Forget what `token` identified, so the next request asks again.
    pub async fn invalidate(&self, token: &str) {
        self.cache.invalidate(token).await;
    }

    /// Forget every token that identified `subject`, so a revoked credential
    /// is asked about again, and refused, on its next request.
    pub fn invalidate_subject(&self, subject: &str) {
        let subject = subject.to_string();
        if let Err(e) = self
            .cache
            .invalidate_entries_if(move |_, principal| principal.subject == subject)
        {
            // Only possible without invalidation closures, which are enabled
            // above; forgetting everything is the safe answer regardless.
            debug!(error = %e, "Forgetting every cached caller instead");
            self.cache.invalidate_all();
        }
    }

    /// Forget every caller.
    pub fn invalidate_all(&self) {
        self.cache.invalidate_all();
    }
}

#[async_trait]
impl Authenticator for CachedAuthenticator {
    async fn authenticate(&self, credential: Credential<'_>) -> Result<Principal, AuthError> {
        let Credential::Bearer(token) = credential else {
            return self.inner.authenticate(credential).await;
        };
        if let Some(principal) = self.cache.get(token).await {
            if still_valid(&principal) {
                debug!(subject = %principal.subject, "Auth cache hit");
                return Ok(principal);
            }
            self.cache.invalidate(token).await;
        }
        let principal = self.inner.authenticate(credential).await?;
        if !still_valid(&principal) {
            return Ok(principal);
        }
        debug!(subject = %principal.subject, "Auth cache miss - cached");
        self.cache
            .insert(token.to_string(), principal.clone())
            .await;
        Ok(principal)
    }
}

/// Whether `principal`'s credential has not expired.
fn still_valid(principal: &Principal) -> bool {
    principal
        .valid_until
        .is_none_or(|until| until > std::time::SystemTime::now())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// Accepts `good`, refuses everything else, and counts what it is asked.
    struct Counting {
        asked: AtomicUsize,
    }

    #[async_trait]
    impl Authenticator for Counting {
        async fn authenticate(&self, credential: Credential<'_>) -> Result<Principal, AuthError> {
            self.asked.fetch_add(1, Ordering::SeqCst);
            match credential {
                Credential::Bearer("good") => Ok(Principal::anonymous()),
                _ => Err(AuthError::Unauthenticated("invalid API key".into())),
            }
        }
    }

    /// Accepts any token, valid until `until`.
    struct ValidUntil(std::time::SystemTime);

    #[async_trait]
    impl Authenticator for ValidUntil {
        async fn authenticate(&self, _credential: Credential<'_>) -> Result<Principal, AuthError> {
            Ok(Principal {
                valid_until: Some(self.0),
                ..Principal::anonymous()
            })
        }
    }

    /// Identifies every token as itself.
    struct Named;

    #[async_trait]
    impl Authenticator for Named {
        async fn authenticate(&self, credential: Credential<'_>) -> Result<Principal, AuthError> {
            let Credential::Bearer(token) = credential else {
                unreachable!()
            };
            Ok(Principal {
                subject: token.to_string(),
                ..Principal::anonymous()
            })
        }
    }

    #[tokio::test]
    async fn forgetting_a_subject_forgets_only_its_tokens() {
        let cached = CachedAuthenticator::new(Arc::new(Named));
        for token in ["key_a", "key_b"] {
            cached
                .authenticate(Credential::Bearer(token))
                .await
                .unwrap();
        }
        cached.invalidate_subject("key_a");
        cached.cache.run_pending_tasks().await;
        assert!(cached.cache.get("key_a").await.is_none());
        assert!(cached.cache.get("key_b").await.is_some());
    }

    #[tokio::test]
    async fn a_token_is_not_remembered_past_its_expiry() {
        let soon = std::time::SystemTime::now() + std::time::Duration::from_millis(200);
        let inner = Arc::new(ValidUntil(soon));
        let cached = CachedAuthenticator::new(inner);
        assert!(cached.authenticate(Credential::Bearer("t")).await.is_ok());
        assert!(cached.cache.get("t").await.is_some());
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        // Expired: forgotten, and asked about again.
        cached.authenticate(Credential::Bearer("t")).await.unwrap();
        assert!(cached.cache.get("t").await.is_none());
    }

    #[tokio::test]
    async fn an_accepted_token_is_asked_about_once_and_a_refused_one_every_time() {
        let inner = Arc::new(Counting {
            asked: AtomicUsize::new(0),
        });
        let cached = CachedAuthenticator::new(inner.clone());

        for _ in 0..3 {
            cached
                .authenticate(Credential::Bearer("good"))
                .await
                .unwrap();
        }
        assert_eq!(inner.asked.load(Ordering::SeqCst), 1);

        for _ in 0..2 {
            assert!(cached
                .authenticate(Credential::Bearer("bad"))
                .await
                .is_err());
        }
        assert_eq!(inner.asked.load(Ordering::SeqCst), 3);

        cached.invalidate("good").await;
        cached
            .authenticate(Credential::Bearer("good"))
            .await
            .unwrap();
        assert_eq!(inner.asked.load(Ordering::SeqCst), 4);
    }
}
