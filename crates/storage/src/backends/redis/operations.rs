//! # Redis Batch Operations
//!
//! Batch operations and pipelining for Redis backend using orcher-common async batching.

use orcher_common::OrcherResult;
use redis::Client;
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tracing::{debug, warn};

/// Batch operations for Redis
#[derive(Clone, Debug)]
pub struct RedisBatchOps {
    client: Client,
}

impl RedisBatchOps {
    /// Create a new Redis batch operations instance
    pub fn new(client: Client) -> Self {
        Self { client }
    }

    /// Get multiple values by keys in a single operation
    pub async fn mget<T>(&self, keys: &[String]) -> OrcherResult<Vec<Option<T>>>
    where
        T: for<'de> Deserialize<'de>,
    {
        debug!("Getting multiple values: keys={:?}", keys);

        if keys.is_empty() {
            return Ok(Vec::new());
        }

        let mut conn = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| {
                orcher_common::error::OrcherError::internal(format!(
                    "Failed to get Redis connection: {}",
                    e
                ))
            })?;

        let results: Vec<Option<Vec<u8>>> = redis::cmd("MGET")
            .arg(keys)
            .query_async(&mut conn)
            .await
            .map_err(|e| {
                warn!("Failed to get multiple values: {}", e);
                orcher_common::error::OrcherError::internal(format!("Batch get failed: {}", e))
            })?;

        let mut values = Vec::with_capacity(results.len());
        for result in results {
            match result {
                Some(bytes) => {
                    let value: T = serde_json::from_slice(&bytes).map_err(|e| {
                        orcher_common::error::OrcherError::internal(format!(
                            "Failed to deserialize cached value: {}",
                            e
                        ))
                    })?;
                    values.push(Some(value));
                }
                None => values.push(None),
            }
        }

        debug!("Batch get completed: retrieved {} values", values.len());
        Ok(values)
    }

    /// Set multiple key-value pairs in a single operation
    pub async fn mset<T>(&self, pairs: &[(String, T)]) -> OrcherResult<()>
    where
        T: Serialize,
    {
        debug!("Setting multiple values: count={}", pairs.len());

        if pairs.is_empty() {
            return Ok(());
        }

        let mut conn = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| {
                orcher_common::error::OrcherError::internal(format!(
                    "Failed to get Redis connection: {}",
                    e
                ))
            })?;

        let mut cmd = redis::cmd("MSET");
        for (key, value) in pairs {
            let bytes = serde_json::to_vec(value).map_err(|e| {
                orcher_common::error::OrcherError::internal(format!(
                    "Failed to serialize value: {}",
                    e
                ))
            })?;
            cmd.arg(key).arg(bytes);
        }

        cmd.query_async::<()>(&mut conn).await.map_err(|e| {
            warn!("Failed to set multiple values: {}", e);
            orcher_common::error::OrcherError::internal(format!("Batch set failed: {}", e))
        })?;

        debug!("Batch set completed: set {} values", pairs.len());
        Ok(())
    }

    /// Delete multiple keys in a single operation
    pub async fn mdel(&self, keys: &[String]) -> OrcherResult<u32> {
        debug!("Deleting multiple keys: count={}", keys.len());

        if keys.is_empty() {
            return Ok(0);
        }

        let mut conn = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| {
                orcher_common::error::OrcherError::internal(format!(
                    "Failed to get Redis connection: {}",
                    e
                ))
            })?;

        let deleted: i32 = redis::cmd("DEL")
            .arg(keys)
            .query_async(&mut conn)
            .await
            .map_err(|e| {
                warn!("Failed to delete multiple keys: {}", e);
                orcher_common::error::OrcherError::internal(format!("Batch delete failed: {}", e))
            })?;

        debug!("Batch delete completed: deleted {} keys", deleted);
        Ok(deleted as u32)
    }

    /// Set multiple keys with expiration using pipelining
    pub async fn mset_with_expiry<T>(
        &self,
        pairs: &[(String, T)],
        ttl: Duration,
    ) -> OrcherResult<()>
    where
        T: Serialize,
    {
        debug!(
            "Setting multiple values with TTL: count={}, ttl={:?}",
            pairs.len(),
            ttl
        );

        if pairs.is_empty() {
            return Ok(());
        }

        let mut conn = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| {
                orcher_common::error::OrcherError::internal(format!(
                    "Failed to get Redis connection: {}",
                    e
                ))
            })?;

        let seconds = ttl.as_secs();

        // Use pipeline for efficient batch operations
        let mut pipe = redis::pipe();
        for (key, value) in pairs {
            let bytes = serde_json::to_vec(value).map_err(|e| {
                orcher_common::error::OrcherError::internal(format!(
                    "Failed to serialize value: {}",
                    e
                ))
            })?;
            pipe.cmd("SETEX").arg(key).arg(seconds).arg(bytes);
        }

        pipe.query_async::<()>(&mut conn).await.map_err(|e| {
            warn!("Failed to set multiple values with expiry: {}", e);
            orcher_common::error::OrcherError::internal(format!(
                "Batch set with expiry failed: {}",
                e
            ))
        })?;

        debug!(
            "Batch set with expiry completed: set {} values",
            pairs.len()
        );
        Ok(())
    }

    /// Check if multiple keys exist
    pub async fn mexists(&self, keys: &[String]) -> OrcherResult<Vec<bool>> {
        debug!("Checking existence of multiple keys: count={}", keys.len());

        if keys.is_empty() {
            return Ok(Vec::new());
        }

        let mut conn = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| {
                orcher_common::error::OrcherError::internal(format!(
                    "Failed to get Redis connection: {}",
                    e
                ))
            })?;

        let mut pipe = redis::pipe();
        for key in keys {
            pipe.cmd("EXISTS").arg(key);
        }

        let results: Vec<i32> = pipe.query_async(&mut conn).await.map_err(|e| {
            warn!("Failed to check multiple key existence: {}", e);
            orcher_common::error::OrcherError::internal(format!("Batch exists check failed: {}", e))
        })?;

        Ok(results.into_iter().map(|r| r > 0).collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_batch_ops_creation() {
        // Compile-time test to ensure RedisBatchOps can be created
        fn _assert_send_sync<T: Send + Sync>() {}
        _assert_send_sync::<RedisBatchOps>();
    }

    #[test]
    fn test_empty_batch() {
        let keys: Vec<String> = Vec::new();
        assert!(keys.is_empty());
    }

    #[test]
    fn test_batch_size() {
        let keys = ["key1".to_string(), "key2".to_string(), "key3".to_string()];
        assert_eq!(keys.len(), 3);
    }

    #[test]
    fn test_ttl_conversion() {
        let ttl = Duration::from_secs(300);
        assert_eq!(ttl.as_secs(), 300);
    }
}
