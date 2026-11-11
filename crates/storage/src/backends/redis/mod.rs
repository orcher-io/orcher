//! # Redis Backend Module
//!
//! Redis backend implementation with connection pooling and health monitoring.
//!
//! Only [`connection`] is load-bearing — it supplies the pool for the optional
//! `orcher-queue` Redis provider. Nothing constructs [`RedisBackend`] itself.

/// Connection management and pooling
pub mod connection;

/// Health monitoring and checks
pub mod health;

/// Batch operations and pipelining
pub mod operations;

// Re-export main types for convenience
pub use connection::{create_pool, PoolStatus, RedisConfig, RedisConnection, RedisConnectionPool};
pub use health::{HealthCheckResult, HealthStatus};
pub use operations::RedisBatchOps;

/// Redis backend with connection pool and health monitoring
#[derive(Debug, Clone)]
pub struct RedisBackend {
    client: redis::Client,
    batch_ops: RedisBatchOps,
}

impl RedisBackend {
    /// Create a new Redis backend with the given configuration
    pub async fn new(config: RedisConfig) -> orcher_common::OrcherResult<Self> {
        let client = connection::create_client(&config).await?;
        let batch_ops = RedisBatchOps::new(client.clone());
        Ok(Self { client, batch_ops })
    }

    /// Get a reference to the Redis client
    pub fn client(&self) -> &redis::Client {
        &self.client
    }

    /// Get a reference to the Redis batch operations
    pub fn batch_ops(&self) -> &RedisBatchOps {
        &self.batch_ops
    }

    /// Perform a health check on the backend
    pub async fn health_check(&self) -> orcher_common::OrcherResult<HealthCheckResult> {
        health::check_health(&self.client).await
    }

    /// Perform a health check with timeout
    pub async fn health_check_with_timeout(
        &self,
        timeout: std::time::Duration,
    ) -> orcher_common::OrcherResult<HealthCheckResult> {
        health::check_health_with_timeout(&self.client, timeout).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_redis_backend_creation() {
        // Compile-time test to ensure types are properly defined
        fn _assert_backend_traits<T: Clone + std::fmt::Debug>() {}
        _assert_backend_traits::<RedisBackend>();
    }
}
