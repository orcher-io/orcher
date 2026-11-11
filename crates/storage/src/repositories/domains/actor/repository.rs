//! # ActorState Domain Repository
//!
//! Actor state persistence implementation using PostgreSQL backend.
//! Handles CRUD operations, filtering, optimistic locking, and analytics for actor state.
//!
//! ## Architecture
//!
//! This repository follows the virtual-objects pattern:
//! - State is scoped by actor instance (actor_name + instance_key)
//! - Optimistic locking via version field
//! - TTL support for automatic cleanup
//! - Pattern-based querying for listing keys
//!
//! ## Usage
//!
//! ```rust,ignore
//! use orcher_storage::repositories::domains::actor::ActorStateRepository;
//! use orcher_storage::schema::types::actor::ActorState;
//!
//! let repo = ActorStateRepository::new(pool);
//!
//! // Store state
//! let key = ActorState::build_key(None, "ShoppingCart", "user-123", "cart");
//! let value = serde_json::to_vec(&cart_data)?;
//! let state = ActorState::new(key, value);
//! repo.create(state).await?;
//!
//! // Retrieve state
//! let state = repo.get_state("ShoppingCart", "user-123", "cart").await?;
//!
//! // Update with optimistic locking
//! repo.update_with_version(key, new_value, expected_version).await?;
//!
//! // List all state keys for an actor instance
//! let keys = repo.list_keys("ShoppingCart", "user-123", None).await?;
//! ```

use async_trait::async_trait;
use jiff::SignedDuration;
use orcher_common::types::time::Timestamptz;
use orcher_common::OrcherResult;
use sqlx::PgPool;
use tracing::{debug, info};

use crate::{
    repositories::base::CrudOperations,
    schema::types::actor::{ActorState, ActorStateFilter, ActorStateStats, ActorTypeStats},
};

/// PostgreSQL implementation of ActorStateRepository
#[derive(Debug, Clone)]
pub struct ActorStateRepository {
    pool: PgPool,
}

impl ActorStateRepository {
    /// Create a new ActorStateRepository instance
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Get a reference to the connection pool
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    // ========== ACTOR STATE OPERATIONS ==========

    /// Get actor state by components
    ///
    /// # Arguments
    ///
    /// * `actor_name` - Actor type name (e.g., "ShoppingCart")
    /// * `instance_key` - Actor instance key (e.g., "user-123")
    /// * `state_key` - State key within the actor (e.g., "cart")
    pub async fn get_state(
        &self,
        organization_id: Option<uuid::Uuid>,
        actor_name: &str,
        instance_key: &str,
        state_key: &str,
    ) -> OrcherResult<Option<ActorState>> {
        let key = ActorState::build_key(organization_id, actor_name, instance_key, state_key);
        self.get_by_key(&key).await
    }

    /// Get actor state by full key
    pub async fn get_by_key(&self, key: &str) -> OrcherResult<Option<ActorState>> {
        debug!("Getting actor state by key: {}", key);

        let result = sqlx::query_as::<_, ActorState>(
            r#"
            SELECT organization_id, key, value, version, created_at, updated_at, size_bytes, expires_at
            FROM actor_state
            WHERE key = $1
            "#,
        )
        .bind(key)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| {
            orcher_common::error::OrcherError::internal(format!("Failed to get actor state: {}", e))
        })?;

        Ok(result)
    }

    /// Set actor state (upsert)
    ///
    /// Creates if doesn't exist, updates if exists.
    pub async fn set_state(
        &self,
        organization_id: Option<uuid::Uuid>,
        actor_name: &str,
        instance_key: &str,
        state_key: &str,
        value: Vec<u8>,
    ) -> OrcherResult<ActorState> {
        let key = ActorState::build_key(organization_id, actor_name, instance_key, state_key);
        self.upsert_by_key(key, value).await
    }

    /// Set actor state with TTL (upsert)
    pub async fn set_state_with_ttl(
        &self,
        organization_id: Option<uuid::Uuid>,
        actor_name: &str,
        instance_key: &str,
        state_key: &str,
        value: Vec<u8>,
        ttl_seconds: i64,
    ) -> OrcherResult<ActorState> {
        let key = ActorState::build_key(organization_id, actor_name, instance_key, state_key);
        let expires_at = Timestamptz::now() + SignedDuration::from_secs(ttl_seconds);
        self.upsert_by_key_with_expiry(key, value, Some(expires_at))
            .await
    }

    /// Upsert actor state by key
    pub async fn upsert_by_key(&self, key: String, value: Vec<u8>) -> OrcherResult<ActorState> {
        debug!("Upserting actor state: key={}, size={}", key, value.len());

        let result = sqlx::query_as::<_, ActorState>(
            r#"
            INSERT INTO actor_state (key, value, version, created_at, updated_at)
            VALUES ($1, $2, 1, NOW(), NOW())
            ON CONFLICT (key)
            DO UPDATE SET
                value = EXCLUDED.value,
                updated_at = NOW(),
                version = actor_state.version + 1
            RETURNING organization_id, key, value, version, created_at, updated_at, size_bytes, expires_at
            "#,
        )
        .bind(&key)
        .bind(&value)
        .fetch_one(&self.pool)
        .await
        .map_err(|e| {
            orcher_common::error::OrcherError::internal(format!(
                "Failed to upsert actor state: {}",
                e
            ))
        })?;

        info!(
            "Actor state upserted: key={}, version={}",
            result.key, result.version
        );

        Ok(result)
    }

    /// Upsert actor state with expiration
    pub async fn upsert_by_key_with_expiry(
        &self,
        key: String,
        value: Vec<u8>,
        expires_at: Option<Timestamptz>,
    ) -> OrcherResult<ActorState> {
        debug!(
            "Upserting actor state with expiry: key={}, size={}, expires_at={:?}",
            key,
            value.len(),
            expires_at
        );

        let result = sqlx::query_as::<_, ActorState>(
            r#"
            INSERT INTO actor_state (key, value, version, created_at, updated_at, expires_at)
            VALUES ($1, $2, 1, NOW(), NOW(), $3)
            ON CONFLICT (key)
            DO UPDATE SET
                value = EXCLUDED.value,
                updated_at = NOW(),
                version = actor_state.version + 1,
                expires_at = EXCLUDED.expires_at
            RETURNING organization_id, key, value, version, created_at, updated_at, size_bytes, expires_at
            "#,
        )
        .bind(&key)
        .bind(&value)
        .bind(expires_at)
        .fetch_one(&self.pool)
        .await
        .map_err(|e| {
            orcher_common::error::OrcherError::internal(format!(
                "Failed to upsert actor state with expiry: {}",
                e
            ))
        })?;

        info!(
            "Actor state upserted with expiry: key={}, version={}",
            result.key, result.version
        );

        Ok(result)
    }

    /// Update actor state with optimistic locking
    ///
    /// Returns error if version mismatch (concurrent modification)
    pub async fn update_with_version(
        &self,
        key: String,
        value: Vec<u8>,
        expected_version: i32,
    ) -> OrcherResult<ActorState> {
        debug!(
            "Updating actor state with version check: key={}, expected_version={}",
            key, expected_version
        );

        let result = sqlx::query_as::<_, ActorState>(
            r#"
            UPDATE actor_state
            SET value = $2, updated_at = NOW(), version = version + 1
            WHERE key = $1 AND version = $3
            RETURNING organization_id, key, value, version, created_at, updated_at, size_bytes, expires_at
            "#,
        )
        .bind(&key)
        .bind(&value)
        .bind(expected_version)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| {
            orcher_common::error::OrcherError::internal(format!(
                "Failed to update actor state: {}",
                e
            ))
        })?;

        match result {
            Some(state) => {
                info!(
                    "Actor state updated: key={}, version={}",
                    state.key, state.version
                );
                Ok(state)
            }
            None => Err(orcher_common::error::OrcherError::invalid_input(format!(
                "Version mismatch for key '{}'. Expected version: {}",
                key, expected_version
            ))),
        }
    }

    /// Delete actor state
    pub async fn delete_state(
        &self,
        organization_id: Option<uuid::Uuid>,
        actor_name: &str,
        instance_key: &str,
        state_key: &str,
    ) -> OrcherResult<bool> {
        let key = ActorState::build_key(organization_id, actor_name, instance_key, state_key);
        self.delete_by_key(&key).await
    }

    /// Delete actor state by key
    pub async fn delete_by_key(&self, key: &str) -> OrcherResult<bool> {
        debug!("Deleting actor state: key={}", key);

        let result = sqlx::query("DELETE FROM actor_state WHERE key = $1")
            .bind(key)
            .execute(&self.pool)
            .await
            .map_err(|e| {
                orcher_common::error::OrcherError::internal(format!(
                    "Failed to delete actor state: {}",
                    e
                ))
            })?;

        let deleted = result.rows_affected() > 0;
        if deleted {
            info!("Actor state deleted: key={}", key);
        } else {
            debug!("Actor state not found for deletion: key={}", key);
        }

        Ok(deleted)
    }

    /// Check if actor state exists
    pub async fn exists_state(
        &self,
        organization_id: Option<uuid::Uuid>,
        actor_name: &str,
        instance_key: &str,
        state_key: &str,
    ) -> OrcherResult<bool> {
        let key = ActorState::build_key(organization_id, actor_name, instance_key, state_key);
        self.exists_by_key(&key).await
    }

    /// Check if actor state exists by key
    pub async fn exists_by_key(&self, key: &str) -> OrcherResult<bool> {
        debug!("Checking if actor state exists: key={}", key);

        let exists: bool =
            sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM actor_state WHERE key = $1)")
                .bind(key)
                .fetch_one(&self.pool)
                .await
                .map_err(|e| {
                    orcher_common::error::OrcherError::internal(format!(
                        "Failed to check actor state existence: {}",
                        e
                    ))
                })?;

        Ok(exists)
    }

    /// List all state keys for an actor instance
    ///
    /// # Arguments
    ///
    /// * `organization_id` - Optional organization ID for multi-tenant filtering
    /// * `actor_name` - Actor type name
    /// * `instance_key` - Actor instance key
    /// * `prefix` - Optional prefix filter for state keys
    pub async fn list_keys(
        &self,
        organization_id: Option<uuid::Uuid>,
        actor_name: &str,
        instance_key: &str,
        prefix: Option<&str>,
    ) -> OrcherResult<Vec<String>> {
        let pattern = format!(
            "{}{}%",
            escape_like(&ActorState::key_prefix(
                organization_id,
                actor_name,
                instance_key
            )),
            escape_like(prefix.unwrap_or_default())
        );

        debug!(
            organization_id = ?organization_id,
            pattern = %pattern,
            "Listing actor state keys"
        );

        // The organisation is matched by the key prefix alone. The row's
        // `organization_id` column is never written — the key is the row's
        // identity and carries the owner — so filtering on the column matched
        // nothing for any organisation.
        let keys: Vec<String> = sqlx::query_scalar(
            r#"
            SELECT key FROM actor_state
            WHERE key LIKE $1 ESCAPE '\'
            ORDER BY key
            "#,
        )
        .bind(&pattern)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| {
            orcher_common::error::OrcherError::internal(format!(
                "Failed to list actor state keys: {}",
                e
            ))
        })?;

        debug!("Found {} actor state keys", keys.len());
        Ok(keys)
    }

    /// Delete all state for an actor instance
    ///
    /// # Arguments
    ///
    /// * `organization_id` - Optional organization ID for multi-tenant filtering
    /// * `actor_name` - Actor type name
    /// * `instance_key` - Actor instance key
    pub async fn delete_all_state(
        &self,
        organization_id: Option<uuid::Uuid>,
        actor_name: &str,
        instance_key: &str,
    ) -> OrcherResult<u64> {
        // Matched by the key prefix alone, for the reason `list_keys` gives.
        let pattern = format!(
            "{}%",
            escape_like(&ActorState::key_prefix(
                organization_id,
                actor_name,
                instance_key
            ))
        );

        debug!(
            organization_id = ?organization_id,
            actor = %actor_name,
            instance = %instance_key,
            "Deleting all actor state"
        );

        let result = sqlx::query(
            r#"
            DELETE FROM actor_state
            WHERE key LIKE $1 ESCAPE '\'
            "#,
        )
        .bind(&pattern)
        .execute(&self.pool)
        .await
        .map_err(|e| {
            orcher_common::error::OrcherError::internal(format!(
                "Failed to delete all actor state: {}",
                e
            ))
        })?;

        let deleted = result.rows_affected();
        info!(
            organization_id = ?organization_id,
            deleted = deleted,
            actor = %actor_name,
            instance = %instance_key,
            "Deleted actor state keys"
        );

        Ok(deleted)
    }

    // ========== FILTERING & QUERYING ==========

    /// Find actor states matching filter criteria
    pub async fn find_by_filter(&self, filter: &ActorStateFilter) -> OrcherResult<Vec<ActorState>> {
        debug!("Finding actor states by filter: {:?}", filter);

        let mut query = String::from(
            "SELECT organization_id, key, value, version, created_at, updated_at, size_bytes, expires_at FROM actor_state WHERE 1=1",
        );
        let mut bind_index = 1;

        // Build dynamic query based on filter
        if filter.key_pattern.is_some() {
            query.push_str(&format!(" AND key LIKE ${}", bind_index));
            bind_index += 1;
        }

        if filter.expired_only {
            query.push_str(" AND expires_at IS NOT NULL AND expires_at < NOW()");
        }

        if filter.valid_only {
            query.push_str(" AND (expires_at IS NULL OR expires_at >= NOW())");
        }

        if filter.min_version.is_some() {
            query.push_str(&format!(" AND version >= ${}", bind_index));
            bind_index += 1;
        }

        if filter.max_version.is_some() {
            query.push_str(&format!(" AND version <= ${}", bind_index));
            bind_index += 1;
        }

        if filter.created_after.is_some() {
            query.push_str(&format!(" AND created_at >= ${}", bind_index));
            bind_index += 1;
        }

        if filter.created_before.is_some() {
            query.push_str(&format!(" AND created_at <= ${}", bind_index));
            bind_index += 1;
        }

        if filter.updated_after.is_some() {
            query.push_str(&format!(" AND updated_at >= ${}", bind_index));
            bind_index += 1;
        }

        if filter.updated_before.is_some() {
            query.push_str(&format!(" AND updated_at <= ${}", bind_index));
            bind_index += 1;
        }

        if filter.min_size_bytes.is_some() {
            query.push_str(&format!(" AND size_bytes >= ${}", bind_index));
            bind_index += 1;
        }

        if filter.max_size_bytes.is_some() {
            query.push_str(&format!(" AND size_bytes <= ${}", bind_index));
            bind_index += 1;
        }

        query.push_str(" ORDER BY key");

        if let Some(_limit) = filter.limit {
            query.push_str(&format!(" LIMIT ${}", bind_index));
            bind_index += 1;
        }

        if let Some(_offset) = filter.offset {
            query.push_str(&format!(" OFFSET ${}", bind_index));
        }

        // Build and execute query with bindings
        let mut query_builder = sqlx::query_as::<_, ActorState>(&query);

        if let Some(ref pattern) = filter.key_pattern {
            query_builder = query_builder.bind(pattern);
        }
        if let Some(min_ver) = filter.min_version {
            query_builder = query_builder.bind(min_ver);
        }
        if let Some(max_ver) = filter.max_version {
            query_builder = query_builder.bind(max_ver);
        }
        if let Some(created_after) = filter.created_after {
            query_builder = query_builder.bind(created_after);
        }
        if let Some(created_before) = filter.created_before {
            query_builder = query_builder.bind(created_before);
        }
        if let Some(updated_after) = filter.updated_after {
            query_builder = query_builder.bind(updated_after);
        }
        if let Some(updated_before) = filter.updated_before {
            query_builder = query_builder.bind(updated_before);
        }
        if let Some(min_size) = filter.min_size_bytes {
            query_builder = query_builder.bind(min_size);
        }
        if let Some(max_size) = filter.max_size_bytes {
            query_builder = query_builder.bind(max_size);
        }
        if let Some(limit) = filter.limit {
            query_builder = query_builder.bind(limit);
        }
        if let Some(offset) = filter.offset {
            query_builder = query_builder.bind(offset);
        }

        let results = query_builder.fetch_all(&self.pool).await.map_err(|e| {
            orcher_common::error::OrcherError::internal(format!(
                "Failed to find actor states by filter: {}",
                e
            ))
        })?;

        debug!("Found {} actor states matching filter", results.len());
        Ok(results)
    }

    // ========== CLEANUP & MAINTENANCE ==========

    // ========== STATISTICS & ANALYTICS ==========

    /// Get statistics for a specific actor instance
    pub async fn get_instance_stats(
        &self,
        actor_name: &str,
        instance_key: &str,
    ) -> OrcherResult<Option<ActorStateStats>> {
        debug!(
            "Getting actor instance stats: actor={}, instance={}",
            actor_name, instance_key
        );

        let result = sqlx::query_as::<_, ActorStateStats>(
            r#"
            SELECT
                split_part(key, ':', 2) AS actor_name,
                split_part(key, ':', 3) AS instance_key,
                COUNT(*)::bigint AS state_count,
                COALESCE(SUM(size_bytes), 0)::bigint AS total_size_bytes,
                COALESCE(AVG(size_bytes), 0)::float8 AS avg_size_bytes,
                COALESCE(MAX(size_bytes), 0)::bigint AS max_size_bytes,
                MAX(updated_at) AS last_updated_at
            FROM actor_state
            WHERE key LIKE $1
            GROUP BY split_part(key, ':', 2), split_part(key, ':', 3)
            "#,
        )
        .bind(format!("actor:{}:{}:%", actor_name, instance_key))
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| {
            orcher_common::error::OrcherError::internal(format!(
                "Failed to get actor instance stats: {}",
                e
            ))
        })?;

        Ok(result)
    }

    /// Get statistics for all instances of an actor type
    pub async fn get_type_stats(&self, actor_name: &str) -> OrcherResult<Option<ActorTypeStats>> {
        debug!("Getting actor type stats: actor={}", actor_name);

        let result = sqlx::query_as::<_, ActorTypeStats>(
            r#"
            SELECT
                split_part(key, ':', 2) AS actor_name,
                COUNT(DISTINCT split_part(key, ':', 3))::bigint AS unique_instances,
                COUNT(*)::bigint AS total_state_keys,
                COALESCE(SUM(size_bytes), 0)::bigint AS total_size_bytes,
                COALESCE(AVG(size_bytes), 0)::float8 AS avg_size_bytes,
                MAX(updated_at) AS last_updated_at
            FROM actor_state
            WHERE key LIKE $1
            GROUP BY split_part(key, ':', 2)
            "#,
        )
        .bind(format!("actor:{}:%", actor_name))
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| {
            orcher_common::error::OrcherError::internal(format!(
                "Failed to get actor type stats: {}",
                e
            ))
        })?;

        Ok(result)
    }

    /// Get total actor state count
    pub async fn count_all(&self) -> OrcherResult<i64> {
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM actor_state")
            .fetch_one(&self.pool)
            .await
            .map_err(|e| {
                orcher_common::error::OrcherError::internal(format!(
                    "Failed to count actor states: {}",
                    e
                ))
            })?;

        Ok(count)
    }

    /// Get total size of all actor state (in bytes)
    pub async fn total_size_bytes(&self) -> OrcherResult<i64> {
        let size: Option<i64> =
            sqlx::query_scalar("SELECT COALESCE(SUM(size_bytes), 0) FROM actor_state")
                .fetch_one(&self.pool)
                .await
                .map_err(|e| {
                    orcher_common::error::OrcherError::internal(format!(
                        "Failed to get total actor state size: {}",
                        e
                    ))
                })?;

        Ok(size.unwrap_or(0))
    }
}

/// `text` as a LIKE pattern that matches only itself.
///
/// Actor names, instance keys and state-key prefixes are the caller's own
/// strings, and `_` and `%` are ordinary characters in them: unescaped,
/// listing `user_1` also listed `userZ1`, and deleting all state of an
/// instance named `%` deleted every instance's. The one wildcard a pattern
/// means is the trailing `%` its caller appends after escaping.
fn escape_like(text: &str) -> String {
    let mut escaped = String::with_capacity(text.len());
    for c in text.chars() {
        if matches!(c, '\\' | '%' | '_') {
            escaped.push('\\');
        }
        escaped.push(c);
    }
    escaped
}

// ========== CRUD TRAIT IMPLEMENTATION ==========

#[async_trait]
impl CrudOperations<ActorState, String> for ActorStateRepository {
    async fn create(&self, entity: ActorState) -> OrcherResult<ActorState> {
        debug!("Creating actor state: key={}", entity.key);

        let result = sqlx::query_as::<_, ActorState>(
            r#"
            INSERT INTO actor_state (key, value, version, created_at, updated_at, expires_at)
            VALUES ($1, $2, $3, $4, $5, $6)
            RETURNING key, value, version, created_at, updated_at, size_bytes, expires_at
            "#,
        )
        .bind(&entity.key)
        .bind(&entity.value)
        .bind(entity.version)
        .bind(entity.created_at)
        .bind(entity.updated_at)
        .bind(entity.expires_at)
        .fetch_one(&self.pool)
        .await
        .map_err(|e| {
            orcher_common::error::OrcherError::internal(format!(
                "Failed to create actor state: {}",
                e
            ))
        })?;

        info!("Actor state created: key={}", result.key);
        Ok(result)
    }

    async fn find_by_id(&self, id: String) -> OrcherResult<Option<ActorState>> {
        self.get_by_key(&id).await
    }

    async fn update(&self, entity: ActorState) -> OrcherResult<ActorState> {
        debug!("Updating actor state: key={}", entity.key);

        let result = sqlx::query_as::<_, ActorState>(
            r#"
            UPDATE actor_state
            SET value = $2, updated_at = $3, expires_at = $4, version = version + 1
            WHERE key = $1
            RETURNING key, value, version, created_at, updated_at, size_bytes, expires_at
            "#,
        )
        .bind(&entity.key)
        .bind(&entity.value)
        .bind(entity.updated_at)
        .bind(entity.expires_at)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| {
            orcher_common::error::OrcherError::internal(format!(
                "Failed to update actor state: {}",
                e
            ))
        })?;

        match result {
            Some(state) => {
                info!("Actor state updated: key={}", state.key);
                Ok(state)
            }
            None => Err(orcher_common::error::OrcherError::not_found(
                "ActorState",
                &entity.key,
            )),
        }
    }

    async fn delete(&self, id: String) -> OrcherResult<bool> {
        self.delete_by_key(&id).await
    }

    async fn exists(&self, id: String) -> OrcherResult<bool> {
        self.exists_by_key(&id).await
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
    fn test_list_pattern() {
        let pattern = format!("actor:{}:{}:%", "ShoppingCart", "user-123");
        assert_eq!(pattern, "actor:ShoppingCart:user-123:%");

        let pattern = format!("actor:{}:{}:{}%", "ShoppingCart", "user-123", "cart_");
        assert_eq!(pattern, "actor:ShoppingCart:user-123:cart_%");
    }

    #[test]
    fn test_delete_all_pattern() {
        let pattern = format!("actor:{}:{}:%", "ShoppingCart", "user-123");
        assert_eq!(pattern, "actor:ShoppingCart:user-123:%");
    }
}
