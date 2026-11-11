//! Actor state storage types.

use jiff::SignedDuration;
use orcher_common::types::time::Timestamptz;
use serde::{Deserialize, Serialize};
use std::fmt;

/// Key format: `actor:{actor_name}:{instance_key}:{state_key}`
#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct ActorState {
    pub organization_id: Option<uuid::Uuid>,
    pub key: String,
    pub value: Vec<u8>,
    /// Incremented on each update (optimistic locking)
    pub version: i32,
    pub created_at: Timestamptz,
    pub updated_at: Timestamptz,
    /// Calculated by database
    #[sqlx(default)]
    pub size_bytes: Option<i32>,
    pub expires_at: Option<Timestamptz>,
}

impl ActorState {
    pub fn new(key: String, value: Vec<u8>) -> Self {
        let now = Timestamptz::now();
        Self {
            organization_id: None,
            key,
            value,
            version: 1,
            created_at: now,
            updated_at: now,
            size_bytes: None, // Calculated by database
            expires_at: None,
        }
    }

    pub fn new_with_org(organization_id: uuid::Uuid, key: String, value: Vec<u8>) -> Self {
        let now = Timestamptz::now();
        Self {
            organization_id: Some(organization_id),
            key,
            value,
            version: 1,
            created_at: now,
            updated_at: now,
            size_bytes: None, // Calculated by database
            expires_at: None,
        }
    }

    pub fn new_with_ttl(key: String, value: Vec<u8>, ttl_seconds: i64) -> Self {
        let now = Timestamptz::now();
        let expires_at = now + SignedDuration::from_secs(ttl_seconds);
        Self {
            organization_id: None,
            key,
            value,
            version: 1,
            created_at: now,
            updated_at: now,
            size_bytes: None,
            expires_at: Some(expires_at),
        }
    }

    /// The row key for a piece of actor state, within an organisation.
    ///
    /// The organisation is part of the key because the key *is* the identity:
    /// the table is keyed on it alone. Without the owner, two tenants running
    /// an actor of the same name with the same instance key — `ShoppingCart`
    /// and `user-123`, say — addressed one row. One tenant's write replaced the
    /// other's state and its reads returned the other's data.
    ///
    /// A caller with no organisation keeps the original format, so a
    /// single-tenant deployment's existing rows stay exactly where they are.
    ///
    /// Until the state RPCs scoped the caller's organisation, every caller's
    /// state was written under that unscoped key, organisation or not. Those
    /// rows are left readable only by callers with no organisation, whose key
    /// they are. An organisation does not fall back to them: nothing records
    /// which organisation wrote such a row — any tenant using the same actor
    /// name and key wrote the same one — so a fallback would hand one
    /// tenant's old state to whichever asked first, the very leak the scoped
    /// key closes. The price is that an organisation starts from empty state
    /// for keys it last wrote before the fix; an operator who knows the owner
    /// can move a row by rewriting its key.
    pub fn build_key(
        organization_id: Option<uuid::Uuid>,
        actor_name: &str,
        instance_key: &str,
        state_key: &str,
    ) -> String {
        match organization_id {
            Some(org) => format!(
                "org:{}:actor:{}:{}:{}",
                org, actor_name, instance_key, state_key
            ),
            None => format!("actor:{}:{}:{}", actor_name, instance_key, state_key),
        }
    }

    /// The prefix every key of one actor instance shares, for listing.
    pub fn key_prefix(
        organization_id: Option<uuid::Uuid>,
        actor_name: &str,
        instance_key: &str,
    ) -> String {
        match organization_id {
            Some(org) => format!("org:{}:actor:{}:{}:", org, actor_name, instance_key),
            None => format!("actor:{}:{}:", actor_name, instance_key),
        }
    }

    /// Returns (actor_name, instance_key, state_key) or None if invalid.
    ///
    /// Accepts both forms: an organisation-scoped key and the unscoped one a
    /// single-tenant deployment writes.
    pub fn parse_key(key: &str) -> Option<(String, String, String)> {
        let parts: Vec<&str> = key.split(':').collect();
        let parts = match parts.as_slice() {
            ["org", _, rest @ ..] => rest.to_vec(),
            other => other.to_vec(),
        };
        if parts.len() == 4 && parts[0] == "actor" {
            Some((
                parts[1].to_string(),
                parts[2].to_string(),
                parts[3].to_string(),
            ))
        } else {
            None
        }
    }

    pub fn actor_name(&self) -> Option<String> {
        Self::parse_key(&self.key).map(|(name, _, _)| name)
    }

    pub fn instance_key(&self) -> Option<String> {
        Self::parse_key(&self.key).map(|(_, key, _)| key)
    }

    pub fn state_key(&self) -> Option<String> {
        Self::parse_key(&self.key).map(|(_, _, state)| state)
    }

    pub fn is_expired(&self) -> bool {
        if let Some(expires_at) = self.expires_at {
            expires_at < Timestamptz::now()
        } else {
            false
        }
    }

    pub fn is_valid(&self) -> bool {
        !self.is_expired()
    }

    pub fn time_until_expiration(&self) -> Option<jiff::SignedDuration> {
        self.expires_at.map(|exp| exp - Timestamptz::now())
    }

    pub fn update_value(&mut self, new_value: Vec<u8>) {
        self.value = new_value;
        self.version += 1;
        self.updated_at = Timestamptz::now();
    }

    pub fn set_expiration(&mut self, expires_at: Timestamptz) {
        self.expires_at = Some(expires_at);
    }

    pub fn set_ttl(&mut self, ttl_seconds: i64) {
        let expires_at = Timestamptz::now() + SignedDuration::from_secs(ttl_seconds);
        self.set_expiration(expires_at);
    }

    pub fn clear_expiration(&mut self) {
        self.expires_at = None;
    }
}

impl fmt::Display for ActorState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "ActorState(key={}, version={}, size={})",
            self.key,
            self.version,
            self.size_bytes.unwrap_or(0)
        )
    }
}

#[derive(Debug, Clone, Default)]
pub struct ActorStateFilter {
    pub actor_name: Option<String>,
    pub instance_key: Option<String>,
    pub state_key: Option<String>,
    pub key_pattern: Option<String>,
    pub expired_only: bool,
    pub valid_only: bool,
    pub min_version: Option<i32>,
    pub max_version: Option<i32>,
    pub created_after: Option<Timestamptz>,
    pub created_before: Option<Timestamptz>,
    pub updated_after: Option<Timestamptz>,
    pub updated_before: Option<Timestamptz>,
    pub min_size_bytes: Option<i32>,
    pub max_size_bytes: Option<i32>,
    pub limit: Option<i64>,
    pub offset: Option<i64>,
}

impl ActorStateFilter {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn actor_name(mut self, name: impl Into<String>) -> Self {
        self.actor_name = Some(name.into());
        self
    }

    pub fn instance_key(mut self, key: impl Into<String>) -> Self {
        self.instance_key = Some(key.into());
        self
    }

    pub fn state_key(mut self, key: impl Into<String>) -> Self {
        self.state_key = Some(key.into());
        self
    }

    pub fn key_pattern(mut self, pattern: impl Into<String>) -> Self {
        self.key_pattern = Some(pattern.into());
        self
    }

    pub fn expired_only(mut self) -> Self {
        self.expired_only = true;
        self.valid_only = false;
        self
    }

    pub fn valid_only(mut self) -> Self {
        self.valid_only = true;
        self.expired_only = false;
        self
    }

    pub fn limit(mut self, limit: i64) -> Self {
        self.limit = Some(limit);
        self
    }

    /// Set result offset
    pub fn offset(mut self, offset: i64) -> Self {
        self.offset = Some(offset);
        self
    }
}

/// Per-instance state storage statistics.
#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct ActorStateStats {
    pub actor_name: String,
    pub instance_key: String,
    pub state_count: i64,
    pub total_size_bytes: i64,
    pub avg_size_bytes: f64,
    pub max_size_bytes: i64,
    pub last_updated_at: Timestamptz,
}

/// Aggregated statistics across all instances of an actor type.
#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct ActorTypeStats {
    pub actor_name: String,
    pub unique_instances: i64,
    pub total_state_keys: i64,
    pub total_size_bytes: i64,
    pub avg_size_bytes: f64,
    pub last_updated_at: Timestamptz,
}

#[cfg(test)]
mod tenancy_tests {
    use super::ActorState;
    use uuid::Uuid;

    /// The collision this closes: the key is the row's identity, so two tenants
    /// running `ShoppingCart` for `user-123` addressed one row — each
    /// overwriting the other's state and reading it back.
    #[test]
    fn two_organisations_address_different_rows() {
        let mine = Uuid::new_v4();
        let theirs = Uuid::new_v4();

        let my_key = ActorState::build_key(Some(mine), "ShoppingCart", "user-123", "cart");
        let their_key = ActorState::build_key(Some(theirs), "ShoppingCart", "user-123", "cart");

        assert_ne!(my_key, their_key);
    }

    /// A single-tenant deployment keeps the original format, so its existing
    /// rows are still addressed by the keys they were written under.
    #[test]
    fn without_an_organisation_the_key_is_unchanged() {
        assert_eq!(
            ActorState::build_key(None, "ShoppingCart", "user-123", "cart"),
            "actor:ShoppingCart:user-123:cart"
        );
    }

    /// Both forms still parse, so name and instance survive the round trip.
    #[test]
    fn a_scoped_key_still_parses() {
        let key = ActorState::build_key(Some(Uuid::new_v4()), "ShoppingCart", "user-123", "cart");
        assert_eq!(
            ActorState::parse_key(&key),
            Some((
                "ShoppingCart".to_string(),
                "user-123".to_string(),
                "cart".to_string()
            ))
        );
    }

    /// Listing an instance's keys must use the same rule, or a scoped write is
    /// invisible to the listing that follows it.
    #[test]
    fn the_listing_prefix_matches_the_keys_it_must_find() {
        let org = Uuid::new_v4();
        let key = ActorState::build_key(Some(org), "ShoppingCart", "user-123", "cart");
        let prefix = ActorState::key_prefix(Some(org), "ShoppingCart", "user-123");
        assert!(
            key.starts_with(&prefix),
            "{key} does not start with {prefix}"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_key() {
        let key = ActorState::build_key(None, "ShoppingCart", "user-123", "cart");
        assert_eq!(key, "actor:ShoppingCart:user-123:cart");
    }

    #[test]
    fn test_parse_key() {
        let key = "actor:ShoppingCart:user-123:cart";
        let parsed = ActorState::parse_key(key);
        assert!(parsed.is_some());

        let (actor_name, instance_key, state_key) = parsed.unwrap();
        assert_eq!(actor_name, "ShoppingCart");
        assert_eq!(instance_key, "user-123");
        assert_eq!(state_key, "cart");
    }

    #[test]
    fn test_parse_key_invalid() {
        let key = "invalid:key:format";
        assert!(ActorState::parse_key(key).is_none());

        let key = "actor:ShoppingCart:user-123"; // Missing state_key
        assert!(ActorState::parse_key(key).is_none());
    }

    #[test]
    fn test_actor_state_new() {
        let key = ActorState::build_key(None, "ShoppingCart", "user-123", "cart");
        let value = b"test_value".to_vec();
        let state = ActorState::new(key.clone(), value.clone());

        assert_eq!(state.key, key);
        assert_eq!(state.value, value);
        assert_eq!(state.version, 1);
        assert!(state.expires_at.is_none());
    }

    #[test]
    fn test_actor_state_with_ttl() {
        let key = ActorState::build_key(None, "ShoppingCart", "user-123", "cart");
        let value = b"test_value".to_vec();
        let ttl = 3600; // 1 hour
        let state = ActorState::new_with_ttl(key, value, ttl);

        assert!(state.expires_at.is_some());
        assert!(!state.is_expired());
    }

    #[test]
    fn test_is_expired() {
        let key = ActorState::build_key(None, "ShoppingCart", "user-123", "cart");
        let value = b"test_value".to_vec();

        // Test with past expiration
        let mut state = ActorState::new(key.clone(), value.clone());
        state.expires_at = Some(Timestamptz::now() - jiff::SignedDuration::from_hours(1));
        assert!(state.is_expired());
        assert!(!state.is_valid());

        // Test with future expiration
        state.expires_at = Some(Timestamptz::now() + jiff::SignedDuration::from_hours(1));
        assert!(!state.is_expired());
        assert!(state.is_valid());

        // Test with no expiration
        state.expires_at = None;
        assert!(!state.is_expired());
        assert!(state.is_valid());
    }

    #[test]
    fn test_update_value() {
        let key = ActorState::build_key(None, "ShoppingCart", "user-123", "cart");
        let value = b"initial".to_vec();
        let mut state = ActorState::new(key, value);

        let initial_version = state.version;
        let initial_updated = state.updated_at;

        // Small delay to ensure timestamp changes
        std::thread::sleep(std::time::Duration::from_millis(10));

        let new_value = b"updated".to_vec();
        state.update_value(new_value.clone());

        assert_eq!(state.value, new_value);
        assert_eq!(state.version, initial_version + 1);
        assert!(state.updated_at > initial_updated);
    }

    #[test]
    fn test_actor_state_getters() {
        let key = ActorState::build_key(None, "ShoppingCart", "user-123", "cart");
        let value = b"test".to_vec();
        let state = ActorState::new(key, value);

        assert_eq!(state.actor_name(), Some("ShoppingCart".to_string()));
        assert_eq!(state.instance_key(), Some("user-123".to_string()));
        assert_eq!(state.state_key(), Some("cart".to_string()));
    }

    #[test]
    fn test_actor_state_filter() {
        let filter = ActorStateFilter::new()
            .actor_name("ShoppingCart")
            .instance_key("user-123")
            .state_key("cart")
            .limit(10);

        assert_eq!(filter.actor_name, Some("ShoppingCart".to_string()));
        assert_eq!(filter.instance_key, Some("user-123".to_string()));
        assert_eq!(filter.state_key, Some("cart".to_string()));
        assert_eq!(filter.limit, Some(10));
    }

    #[test]
    fn test_filter_expired_valid() {
        let filter = ActorStateFilter::new().expired_only();
        assert!(filter.expired_only);
        assert!(!filter.valid_only);

        let filter = ActorStateFilter::new().valid_only();
        assert!(filter.valid_only);
        assert!(!filter.expired_only);
    }
}
