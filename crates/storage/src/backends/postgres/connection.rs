//! # PostgreSQL Connection Management
//!
//! Connection pooling and management for PostgreSQL backend using orcher-common utilities.

use orcher_common::OrcherResult;
use sqlx::postgres::{PgPool, PgPoolOptions};
use std::time::Duration;
use tracing::{debug, info};

/// PostgreSQL connection configuration
#[derive(Debug, Clone)]
pub struct PostgresConfig {
    /// Database connection URL
    pub url: String,
    /// Maximum number of connections in the pool
    pub max_connections: u32,
    /// Minimum number of connections in the pool
    pub min_connections: u32,
    /// Connection timeout
    pub connect_timeout: Duration,
    /// Maximum lifetime of a connection
    pub max_lifetime: Duration,
    /// Idle timeout for connections
    pub idle_timeout: Duration,
}

impl Default for PostgresConfig {
    fn default() -> Self {
        Self {
            url: String::from("postgres://localhost/orcher"),
            max_connections: 10,
            min_connections: 2,
            connect_timeout: Duration::from_secs(30),
            max_lifetime: Duration::from_secs(1800),
            idle_timeout: Duration::from_secs(600),
        }
    }
}

/// PostgreSQL connection wrapper
#[derive(Debug, Clone)]
pub struct PostgresConnection {
    pool: PgPool,
}

impl PostgresConnection {
    /// Get a reference to the connection pool
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }
}

/// Create a new PostgreSQL connection pool with the given configuration
pub async fn create_pool(config: &PostgresConfig) -> OrcherResult<PgPool> {
    info!("Creating PostgreSQL connection pool");
    debug!(
        "Config: max_connections={}, min_connections={}, connect_timeout={:?}",
        config.max_connections, config.min_connections, config.connect_timeout
    );

    let pool = crate::stale_plan::recycle_stale_connections(PgPoolOptions::new())
        .max_connections(config.max_connections)
        .min_connections(config.min_connections)
        .acquire_timeout(config.connect_timeout)
        .max_lifetime(Some(config.max_lifetime))
        .idle_timeout(Some(config.idle_timeout))
        .connect(&config.url)
        .await
        .map_err(|e| {
            orcher_common::error::OrcherError::internal(format!(
                "Failed to create PostgreSQL connection pool: {}",
                e
            ))
        })?;

    info!("PostgreSQL connection pool created successfully");
    Ok(pool)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = PostgresConfig::default();
        assert_eq!(config.max_connections, 10);
        assert_eq!(config.min_connections, 2);
        assert!(config.url.contains("postgres://"));
    }

    #[test]
    fn test_config_timeouts() {
        let config = PostgresConfig::default();
        assert_eq!(config.connect_timeout, Duration::from_secs(30));
        assert_eq!(config.max_lifetime, Duration::from_secs(1800));
        assert_eq!(config.idle_timeout, Duration::from_secs(600));
    }
}
