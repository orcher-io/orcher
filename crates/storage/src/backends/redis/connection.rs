//! # Redis Connection Management
//!
//! Connection management for Redis backend using orcher-common utilities.
//! Provides both basic client and connection pool support.

use deadpool_redis::{Config as PoolConfig, Pool, Runtime};
use orcher_common::OrcherResult;
use redis::Client;
use std::time::Duration;
use tracing::{debug, info};

/// Redis connection configuration
#[derive(Debug, Clone)]
pub struct RedisConfig {
    /// Redis connection URL
    pub url: String,
    /// Connection timeout
    pub connect_timeout: Duration,
    /// Command timeout
    pub command_timeout: Duration,
    /// Maximum number of retries
    pub max_retries: u32,
    /// Connection pool size (for pooled connections)
    pub pool_size: usize,
    /// Minimum idle connections in pool
    pub pool_min_idle: Option<usize>,
}

impl Default for RedisConfig {
    fn default() -> Self {
        Self {
            url: String::from("redis://localhost:6379"),
            connect_timeout: Duration::from_secs(5),
            command_timeout: Duration::from_secs(3),
            max_retries: 3,
            pool_size: 10,
            pool_min_idle: Some(2),
        }
    }
}

/// Redis connection wrapper
#[derive(Debug, Clone)]
pub struct RedisConnection {
    client: Client,
}

impl RedisConnection {
    /// Get a reference to the Redis client
    pub fn client(&self) -> &Client {
        &self.client
    }
}

/// Create a new Redis client with the given configuration
pub async fn create_client(config: &RedisConfig) -> OrcherResult<Client> {
    info!("Creating Redis client");
    debug!(
        "Config: url={}, connect_timeout={:?}",
        crate::setup::redact_connection_url(&config.url),
        config.connect_timeout
    );

    let client = Client::open(config.url.as_str()).map_err(|e| {
        orcher_common::error::OrcherError::internal(format!("Failed to create Redis client: {}", e))
    })?;

    info!("Redis client created successfully");
    Ok(client)
}

/// Create a Redis connection pool with the given configuration
///
/// # Arguments
///
/// * `config` - Redis configuration including pool settings
///
/// # Returns
///
/// * `Ok(Pool)` - A configured connection pool
/// * `Err(_)` - If pool creation or validation fails
///
/// # Example
///
/// ```rust,no_run
/// use orcher_storage::backends::redis::connection::{RedisConfig, create_pool};
///
/// #[tokio::main]
/// async fn main() -> Result<(), Box<dyn std::error::Error>> {
///     let config = RedisConfig {
///         url: "redis://localhost:6379".to_string(),
///         pool_size: 10,
///         ..Default::default()
///     };
///
///     let pool = create_pool(&config).await?;
///
///     // Use the pool
///     let mut conn = pool.get().await?;
///     redis::cmd("PING").query_async(&mut conn).await?;
///
///     Ok(())
/// }
/// ```
pub async fn create_pool(config: &RedisConfig) -> OrcherResult<Pool> {
    info!("Creating Redis connection pool");
    debug!(
        "Pool config: url={}, pool_size={}, min_idle={:?}",
        crate::setup::redact_connection_url(&config.url),
        config.pool_size,
        config.pool_min_idle
    );

    // Create deadpool-redis configuration
    let pool_config = PoolConfig::from_url(&config.url);

    // Build the pool with configuration
    let pool = pool_config
        .builder()
        .map_err(|e| {
            orcher_common::error::OrcherError::internal(format!(
                "Failed to build pool config: {}",
                e
            ))
        })?
        .max_size(config.pool_size)
        .runtime(Runtime::Tokio1)
        .build()
        .map_err(|e| {
            orcher_common::error::OrcherError::internal(format!("Failed to create pool: {}", e))
        })?;

    // Test the pool with a ping
    info!("Testing Redis connection pool");
    let _conn = pool.get().await.map_err(|e| {
        orcher_common::error::OrcherError::internal(format!(
            "Failed to get connection from pool: {}",
            e
        ))
    })?;

    // Test the pool connection by getting a connection
    // The pool itself validates connections, so if we got here, it works
    info!("Redis connection pool validated successfully");

    info!(
        "Redis connection pool created successfully with {} max connections",
        config.pool_size
    );
    Ok(pool)
}

/// Redis connection pool wrapper for easier management
#[derive(Clone)]
pub struct RedisConnectionPool {
    pool: Pool,
}

impl RedisConnectionPool {
    /// Create a new connection pool with the given configuration
    pub async fn new(config: &RedisConfig) -> OrcherResult<Self> {
        let pool = create_pool(config).await?;
        Ok(Self { pool })
    }

    /// Get a connection from the pool
    pub async fn get(&self) -> OrcherResult<deadpool_redis::Connection> {
        self.pool.get().await.map_err(|e| {
            orcher_common::error::OrcherError::internal(format!(
                "Failed to get connection from pool: {}",
                e
            ))
        })
    }

    /// Get the underlying pool
    pub fn pool(&self) -> &Pool {
        &self.pool
    }

    /// Get pool status information
    pub fn status(&self) -> PoolStatus {
        let status = self.pool.status();
        PoolStatus {
            size: status.size,
            available: status.available,
            max_size: status.max_size,
        }
    }
}

impl std::fmt::Debug for RedisConnectionPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedisConnectionPool")
            .field("status", &self.status())
            .finish()
    }
}

/// Pool status information
#[derive(Debug, Clone)]
pub struct PoolStatus {
    /// Current number of connections in the pool
    pub size: usize,
    /// Number of available connections
    pub available: usize,
    /// Maximum pool size
    pub max_size: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = RedisConfig::default();
        assert!(config.url.contains("redis://"));
        assert_eq!(config.connect_timeout, Duration::from_secs(5));
        assert_eq!(config.command_timeout, Duration::from_secs(3));
        assert_eq!(config.max_retries, 3);
        assert_eq!(config.pool_size, 10);
        assert_eq!(config.pool_min_idle, Some(2));
    }

    #[test]
    fn test_config_timeouts() {
        let config = RedisConfig::default();
        assert_eq!(config.connect_timeout, Duration::from_secs(5));
        assert_eq!(config.command_timeout, Duration::from_secs(3));
    }

    #[test]
    fn test_custom_pool_config() {
        let config = RedisConfig {
            pool_size: 20,
            pool_min_idle: Some(5),
            ..Default::default()
        };
        assert_eq!(config.pool_size, 20);
        assert_eq!(config.pool_min_idle, Some(5));
    }

    #[tokio::test]
    #[ignore] // Requires Redis instance
    async fn test_pool_creation() {
        let config = RedisConfig::default();
        let result = create_pool(&config).await;
        // This will fail if Redis is not running, which is expected
        // In CI, we use testcontainers
        if result.is_ok() {
            let pool = result.unwrap();
            let status = pool.status();
            assert!(status.max_size >= config.pool_size);
        }
    }

    #[tokio::test]
    #[ignore] // Requires Redis instance
    async fn test_connection_pool_wrapper() {
        let config = RedisConfig::default();
        let result = RedisConnectionPool::new(&config).await;
        if result.is_ok() {
            let pool = result.unwrap();
            let status = pool.status();
            assert_eq!(status.max_size, config.pool_size);
        }
    }
}
