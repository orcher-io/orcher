//! Actor state storage, backed by PostgreSQL.

use async_trait::async_trait;
use orcher_common::types::time::Timestamptz;
use orcher_common::OrcherResult;
use orcher_storage::{
    repositories::domains::actor::ActorStateRepository,
    schema::types::actor::{ActorState, ActorStateFilter, ActorStateStats, ActorTypeStats},
};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use std::sync::Arc;
use tracing::{debug, error, warn};

/// Actor state storage interface
///
/// This trait defines the high-level operations for storing and retrieving actor state.
#[async_trait]
pub trait StateStorage: Send + Sync {
    /// Get actor state
    async fn get(&self, key: &str) -> OrcherResult<Option<Vec<u8>>>;

    /// Set actor state
    async fn set(&self, key: &str, value: Vec<u8>) -> OrcherResult<()>;

    /// Delete actor state
    async fn delete(&self, key: &str) -> OrcherResult<bool>;

    /// Check if state exists
    async fn exists(&self, key: &str) -> OrcherResult<bool>;

    /// List all keys matching a pattern
    async fn list_keys(&self, pattern: &str) -> OrcherResult<Vec<String>>;
}

/// Two-tier actor state storage using Redis + PostgreSQL
///
/// This implementation provides:
/// - Fast access via Redis cache
/// - Durable persistence via PostgreSQL (using repository pattern)
/// - Automatic cache-aside pattern
/// - Fault tolerance (falls back to PostgreSQL if Redis fails)
///
/// ## Key Format
///
/// State keys follow the pattern: `actor:{actor_name}:{key}:{state_key}`
///
/// Example: `actor:ShoppingCart:user-123:cart`
pub struct ActorStateStorage {
    /// PostgreSQL repository — the only store. There was a Redis cache in front
    /// of this; it was removed because the read path could not use it. Reads must
    /// return an accurate `version` for optimistic locking, which only the
    /// repository has, so `get_state` always went to PostgreSQL and the cache
    /// only ever absorbed writes.
    ///
    /// A cache here is still worth having — but an in-process, version-aware one,
    /// which is a different design, not a reconnected Redis.
    repository: Arc<ActorStateRepository>,

    /// Whether to enable write-through to PostgreSQL
    enable_postgres_persistence: bool,
}

impl ActorStateStorage {
    /// Create a new actor state storage
    ///
    /// # Arguments
    ///
    /// * `pool` - PostgreSQL connection pool
    pub fn new(pool: PgPool) -> Self {
        Self {
            repository: Arc::new(ActorStateRepository::new(pool)),
            enable_postgres_persistence: true,
        }
    }

    /// Create a new actor state storage with configuration
    ///
    /// # Arguments
    ///
    /// * `pool` - PostgreSQL connection pool
    /// * `enable_postgres` - Whether to persist to PostgreSQL
    pub fn new_with_config(pool: PgPool, enable_postgres: bool) -> Self {
        Self {
            repository: Arc::new(ActorStateRepository::new(pool)),
            enable_postgres_persistence: enable_postgres,
        }
    }

    /// Get actor state (raw bytes) - for gRPC handler
    ///
    /// # Arguments
    ///
    /// * `actor_name` - Actor type name (e.g., "ShoppingCart")
    /// * `key` - Actor instance key (e.g., "user-123")
    /// * `state_key` - State key within the actor (e.g., "cart")
    pub async fn get_state(
        &self,
        actor_name: &str,
        key: &str,
        state_key: &str,
    ) -> OrcherResult<Option<orcher_storage::schema::types::actor::ActorState>> {
        // Scoped to the caller's organisation, because the key is the row's
        // identity: two tenants running the same actor with the same instance
        // key addressed one row, and each overwrote and read the other's state.
        let organization_id = crate::grpc::tenant::current_organization();
        let full_key = ActorState::build_key(organization_id, actor_name, key, state_key);
        debug!("Getting actor state: key={}", full_key);

        // Always read from PostgreSQL when persistence is enabled so that callers
        // receive an accurate `version` for optimistic locking.  Cache is used as
        // a write-through warm-up only (not as a read source).
        if self.enable_postgres_persistence {
            match self.repository.get_by_key(&full_key).await {
                Ok(Some(state)) => {
                    debug!("PostgreSQL hit for actor state: key={}", full_key);
                    return Ok(Some(state));
                }
                Ok(None) => {
                    debug!("PostgreSQL miss for actor state: key={}", full_key);
                    return Ok(None);
                }
                Err(e) => {
                    error!("Failed to get from PostgreSQL repository: {}", e);
                    return Err(e);
                }
            }
        }

        // PostgreSQL disabled — nothing else stores actor state, so there is
        // nothing to return. Previously this fell through to the cache, which
        // could only ever answer with `version: 0` and so was unusable for the
        // optimistic locking every caller relies on.
        Ok(None)
    }

    /// Set actor state with optimistic locking.
    ///
    /// Only succeeds if the current DB version matches `expected_version`.
    /// Returns `Err` with an `invalid_input` code on version conflict so the
    /// caller can retry with a fresh `get_state` → modify → `set_state_with_version`
    /// cycle.
    pub async fn set_state_with_version(
        &self,
        actor_name: &str,
        key: &str,
        state_key: &str,
        value: &[u8],
        expected_version: i32,
    ) -> OrcherResult<orcher_storage::schema::types::actor::ActorState> {
        // Scoped to the caller's organisation, because the key is the row's
        // identity: two tenants running the same actor with the same instance
        // key addressed one row, and each overwrote and read the other's state.
        let organization_id = crate::grpc::tenant::current_organization();
        let full_key = ActorState::build_key(organization_id, actor_name, key, state_key);
        debug!(
            "Setting actor state with version check: key={}, expected_version={}",
            full_key, expected_version
        );

        let state = self
            .repository
            .update_with_version(full_key.clone(), value.to_vec(), expected_version)
            .await?;

        Ok(state)
    }

    /// Set actor state (raw bytes) - for gRPC handler
    ///
    /// # Arguments
    ///
    /// * `actor_name` - Actor type name
    /// * `key` - Actor instance key
    /// * `state_key` - State key within the actor
    /// * `value` - Raw bytes to store
    pub async fn set_state(
        &self,
        actor_name: &str,
        key: &str,
        state_key: &str,
        value: &[u8],
    ) -> OrcherResult<orcher_storage::schema::types::actor::ActorState> {
        // Scoped to the caller's organisation, because the key is the row's
        // identity: two tenants running the same actor with the same instance
        // key addressed one row, and each overwrote and read the other's state.
        let organization_id = crate::grpc::tenant::current_organization();
        let full_key = ActorState::build_key(organization_id, actor_name, key, state_key);
        debug!(
            "Setting actor state: key={}, size={} bytes",
            full_key,
            value.len()
        );

        // Write to PostgreSQL (durable) if enabled using repository
        if self.enable_postgres_persistence {
            let state = self
                .repository
                .upsert_by_key(full_key.clone(), value.to_vec())
                .await?;
            debug!("Actor state persisted to PostgreSQL: key={}", full_key);
            Ok(state)
        } else {
            // If PostgreSQL is disabled, return a placeholder state
            Ok(ActorState {
                organization_id: None, // PostgreSQL disabled, no org context
                key: full_key,
                value: value.to_vec(),
                version: 1,
                created_at: Timestamptz::now(),
                updated_at: Timestamptz::now(),
                size_bytes: Some(value.len() as i32),
                expires_at: None,
            })
        }
    }

    /// Delete actor state - for gRPC handler
    ///
    /// # Arguments
    ///
    /// * `actor_name` - Actor type name
    /// * `key` - Actor instance key
    /// * `state_key` - State key within the actor
    pub async fn delete_state(
        &self,
        actor_name: &str,
        key: &str,
        state_key: &str,
    ) -> OrcherResult<bool> {
        self.delete(actor_name, key, state_key).await
    }

    /// Get typed state from storage
    ///
    /// # Arguments
    ///
    /// * `actor_name` - Actor type name (e.g., "ShoppingCart")
    /// * `key` - Actor instance key (e.g., "user-123")
    /// * `state_key` - State key within the actor (e.g., "cart")
    pub async fn get<T>(
        &self,
        actor_name: &str,
        key: &str,
        state_key: &str,
    ) -> OrcherResult<Option<T>>
    where
        T: for<'de> Deserialize<'de>,
    {
        // Scoped to the caller's organisation, because the key is the row's
        // identity: two tenants running the same actor with the same instance
        // key addressed one row, and each overwrote and read the other's state.
        let organization_id = crate::grpc::tenant::current_organization();
        let full_key = ActorState::build_key(organization_id, actor_name, key, state_key);
        debug!("Getting typed actor state: key={}", full_key);

        match self.get_raw(&full_key).await? {
            Some(bytes) => {
                let value = serde_json::from_slice(&bytes).map_err(|e| {
                    orcher_common::error::OrcherError::internal(format!(
                        "Failed to deserialize actor state: {}",
                        e
                    ))
                })?;
                Ok(Some(value))
            }
            None => Ok(None),
        }
    }

    /// Set typed state in storage
    ///
    /// # Arguments
    ///
    /// * `actor_name` - Actor type name (e.g., "ShoppingCart")
    /// * `key` - Actor instance key (e.g., "user-123")
    /// * `state_key` - State key within the actor (e.g., "cart")
    /// * `value` - The value to store
    pub async fn set<T>(
        &self,
        actor_name: &str,
        key: &str,
        state_key: &str,
        value: &T,
    ) -> OrcherResult<()>
    where
        T: Serialize,
    {
        // Scoped to the caller's organisation, because the key is the row's
        // identity: two tenants running the same actor with the same instance
        // key addressed one row, and each overwrote and read the other's state.
        let organization_id = crate::grpc::tenant::current_organization();
        let full_key = ActorState::build_key(organization_id, actor_name, key, state_key);
        debug!("Setting typed actor state: key={}", full_key);

        let bytes = serde_json::to_vec(value).map_err(|e| {
            orcher_common::error::OrcherError::internal(format!(
                "Failed to serialize actor state: {}",
                e
            ))
        })?;

        self.set_raw(full_key, bytes).await
    }

    /// Get raw bytes from storage
    async fn get_raw(&self, key: &str) -> OrcherResult<Option<Vec<u8>>> {
        debug!("Getting raw actor state: key={}", key);

        // PostgreSQL is the only store.
        if self.enable_postgres_persistence {
            match self.repository.get_by_key(key).await {
                Ok(Some(state)) => {
                    debug!("PostgreSQL hit for actor state: key={}", key);

                    return Ok(Some(state.value));
                }
                Ok(None) => {
                    debug!("PostgreSQL miss for actor state: key={}", key);
                    return Ok(None);
                }
                Err(e) => {
                    error!("Failed to get from PostgreSQL repository: {}", e);
                    return Err(e);
                }
            }
        }

        Ok(None)
    }

    /// Set raw bytes in storage
    async fn set_raw(&self, key: String, value: Vec<u8>) -> OrcherResult<()> {
        debug!(
            "Setting raw actor state: key={}, size={} bytes",
            key,
            value.len()
        );

        // Write to cache first (fast)

        // Write to PostgreSQL (durable) if enabled using repository
        if self.enable_postgres_persistence {
            self.repository.upsert_by_key(key.clone(), value).await?;
            debug!("Actor state persisted to PostgreSQL: key={}", key);
        }

        Ok(())
    }

    /// Delete state
    ///
    /// # Arguments
    ///
    /// * `actor_name` - Actor type name
    /// * `key` - Actor instance key
    /// * `state_key` - State key within the actor
    pub async fn delete(&self, actor_name: &str, key: &str, state_key: &str) -> OrcherResult<bool> {
        // Scoped to the caller's organisation, because the key is the row's
        // identity: two tenants running the same actor with the same instance
        // key addressed one row, and each overwrote and read the other's state.
        let organization_id = crate::grpc::tenant::current_organization();
        let full_key = ActorState::build_key(organization_id, actor_name, key, state_key);
        self.delete_raw(&full_key).await
    }

    /// Delete raw state
    async fn delete_raw(&self, key: &str) -> OrcherResult<bool> {
        debug!("Deleting actor state: key={}", key);

        let mut deleted = false;

        // Delete from PostgreSQL using repository
        if self.enable_postgres_persistence {
            match self.repository.delete_by_key(key).await {
                Ok(pg_deleted) => {
                    deleted = deleted || pg_deleted;
                    debug!(
                        "Deleted from PostgreSQL: key={}, existed={}",
                        key, pg_deleted
                    );
                }
                Err(e) => {
                    error!("Failed to delete from PostgreSQL repository: {}", e);
                    return Err(e);
                }
            }
        }

        Ok(deleted)
    }

    /// Check if state exists
    ///
    /// # Arguments
    ///
    /// * `actor_name` - Actor type name
    /// * `key` - Actor instance key
    /// * `state_key` - State key within the actor
    pub async fn exists(&self, actor_name: &str, key: &str, state_key: &str) -> OrcherResult<bool> {
        // Scoped to the caller's organisation, because the key is the row's
        // identity: two tenants running the same actor with the same instance
        // key addressed one row, and each overwrote and read the other's state.
        let organization_id = crate::grpc::tenant::current_organization();
        let full_key = ActorState::build_key(organization_id, actor_name, key, state_key);
        self.exists_raw(&full_key).await
    }

    /// Check if raw state exists
    async fn exists_raw(&self, key: &str) -> OrcherResult<bool> {
        debug!("Checking if actor state exists: key={}", key);

        // Check PostgreSQL using repository
        if self.enable_postgres_persistence {
            let exists = self.repository.exists_by_key(key).await?;
            debug!("PostgreSQL exists check: key={}, exists={}", key, exists);
            return Ok(exists);
        }

        Ok(false)
    }

    /// List all state keys for an actor instance
    ///
    /// # Arguments
    ///
    /// * `organization_id` - Optional organization ID for multi-tenant filtering
    /// * `actor_name` - Actor type name
    /// * `key` - Actor instance key
    /// * `prefix` - Optional prefix filter for state keys
    pub async fn list_keys(
        &self,
        organization_id: Option<uuid::Uuid>,
        actor_name: &str,
        key: &str,
        prefix: Option<&str>,
    ) -> OrcherResult<Vec<String>> {
        debug!(
            organization_id = ?organization_id,
            actor = %actor_name,
            key = %key,
            prefix = ?prefix,
            "Listing actor state keys"
        );

        // For listing, we use PostgreSQL as source of truth via repository
        if self.enable_postgres_persistence {
            self.repository
                .list_keys(organization_id, actor_name, key, prefix)
                .await
        } else {
            // If PostgreSQL is disabled, return empty list
            warn!("PostgreSQL persistence disabled, cannot list keys efficiently");
            Ok(Vec::new())
        }
    }

    /// Delete all state for an actor instance
    ///
    /// # Arguments
    ///
    /// * `organization_id` - Optional organization ID for multi-tenant filtering
    /// * `actor_name` - Actor type name
    /// * `key` - Actor instance key
    pub async fn delete_all_state(
        &self,
        organization_id: Option<uuid::Uuid>,
        actor_name: &str,
        key: &str,
    ) -> OrcherResult<u64> {
        debug!(
            organization_id = ?organization_id,
            actor = %actor_name,
            instance = %key,
            "Deleting all actor state"
        );

        if self.enable_postgres_persistence {
            // Use repository to delete from PostgreSQL
            let deleted = self
                .repository
                .delete_all_state(organization_id, actor_name, key)
                .await?;

            Ok(deleted)
        } else {
            Ok(0)
        }
    }

    /// Get statistics for a specific actor instance
    pub async fn get_instance_stats(
        &self,
        actor_name: &str,
        instance_key: &str,
    ) -> OrcherResult<Option<ActorStateStats>> {
        self.repository
            .get_instance_stats(actor_name, instance_key)
            .await
    }

    /// Get statistics for all instances of an actor type
    pub async fn get_type_stats(&self, actor_name: &str) -> OrcherResult<Option<ActorTypeStats>> {
        self.repository.get_type_stats(actor_name).await
    }

    /// Get total actor state count
    pub async fn count_all(&self) -> OrcherResult<i64> {
        self.repository.count_all().await
    }

    /// Get total size of all actor state (in bytes)
    pub async fn total_size_bytes(&self) -> OrcherResult<i64> {
        self.repository.total_size_bytes().await
    }

    /// Get reference to the underlying repository
    pub fn repository(&self) -> &ActorStateRepository {
        &self.repository
    }
}

#[async_trait]
impl StateStorage for ActorStateStorage {
    async fn get(&self, key: &str) -> OrcherResult<Option<Vec<u8>>> {
        self.get_raw(key).await
    }

    async fn set(&self, key: &str, value: Vec<u8>) -> OrcherResult<()> {
        self.set_raw(key.to_string(), value).await
    }

    async fn delete(&self, key: &str) -> OrcherResult<bool> {
        self.delete_raw(key).await
    }

    async fn exists(&self, key: &str) -> OrcherResult<bool> {
        self.exists_raw(key).await
    }

    async fn list_keys(&self, pattern: &str) -> OrcherResult<Vec<String>> {
        // Parse pattern to extract actor_name and instance_key
        // Expected pattern: actor:{actor_name}:{instance_key}:*
        if let Some((actor_name, instance_key, prefix)) = parse_pattern(pattern) {
            // Note: StateStorage trait doesn't have organization context,
            // so we pass None. Callers needing org filtering should use
            // ActorStateStorage::list_keys directly.
            ActorStateStorage::list_keys(self, None, &actor_name, &instance_key, prefix.as_deref())
                .await
        } else {
            // Fallback: use repository filter
            let filter = ActorStateFilter::new().key_pattern(pattern);
            let results = self.repository.find_by_filter(&filter).await?;
            Ok(results.into_iter().map(|s| s.key).collect())
        }
    }
}

/// Parse a key pattern into components
///
/// Pattern format: `actor:{actor_name}:{instance_key}:{prefix}*`
///
/// Returns (actor_name, instance_key, prefix_opt)
fn parse_pattern(pattern: &str) -> Option<(String, String, Option<String>)> {
    let parts: Vec<&str> = pattern.split(':').collect();
    if parts.len() >= 3 && parts[0] == "actor" {
        let actor_name = parts[1].to_string();
        let instance_key = parts[2].to_string();

        let prefix = if parts.len() == 4 {
            // Remove trailing wildcard if present
            let prefix_part = parts[3].trim_end_matches('*').trim_end_matches('%');
            if prefix_part.is_empty() {
                None
            } else {
                Some(prefix_part.to_string())
            }
        } else {
            None
        };

        Some((actor_name, instance_key, prefix))
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_pattern() {
        let (actor, key, prefix) =
            parse_pattern("actor:ShoppingCart:user-123:*").expect("Parse failed");
        assert_eq!(actor, "ShoppingCart");
        assert_eq!(key, "user-123");
        assert_eq!(prefix, None);

        let (actor, key, prefix) =
            parse_pattern("actor:ShoppingCart:user-123:cart*").expect("Parse failed");
        assert_eq!(actor, "ShoppingCart");
        assert_eq!(key, "user-123");
        assert_eq!(prefix, Some("cart".to_string()));

        let (actor, key, prefix) =
            parse_pattern("actor:ShoppingCart:user-123:cart_%").expect("Parse failed");
        assert_eq!(actor, "ShoppingCart");
        assert_eq!(key, "user-123");
        assert_eq!(prefix, Some("cart_".to_string()));
    }

    #[test]
    fn test_parse_pattern_invalid() {
        assert!(parse_pattern("invalid:pattern").is_none());
        assert!(parse_pattern("actor:ShoppingCart").is_none());
    }
}
