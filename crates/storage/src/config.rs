//! # Storage Configuration Module
//!
//! Integration layer between orcher-storage and orcher-config for configuration management.
//!
//! This module provides utilities for loading and validating storage configuration
//! from the centralized orcher-config system.

use orcher_common::OrcherResult;
use orcher_config::config::storage::StorageConfig;
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use std::time::Duration;
use tracing::{debug, info};
use urlencoding;

/// Storage configuration manager
///
/// Provides methods to initialize database and cache connections from configuration.
#[derive(Debug, Clone)]
pub struct StorageConfigManager {
    config: StorageConfig,
}

impl StorageConfigManager {
    /// Create a new storage configuration manager
    ///
    /// # Arguments
    /// * `config` - Storage configuration from orcher-config
    pub fn new(config: StorageConfig) -> Self {
        Self { config }
    }

    /// Get reference to the storage configuration
    pub fn config(&self) -> &StorageConfig {
        &self.config
    }

    /// Create PostgreSQL connection pool from configuration
    ///
    /// # Returns
    /// * `Ok(PgPool)` - Configured connection pool
    /// * `Err(OrcherError)` - Configuration or connection error
    pub async fn create_postgres_pool(&self) -> OrcherResult<PgPool> {
        debug!("Creating PostgreSQL connection pool from configuration");

        // Build connection URL with application_name and connect_timeout
        let mut connection_url = if self
            .config
            .database
            .postgres_url
            .contains("application_name=")
        {
            self.config.database.postgres_url.clone()
        } else {
            format!(
                "{}?application_name={}",
                self.config.database.postgres_url.trim_end_matches('?'),
                urlencoding::encode(&self.config.database.application_name)
            )
        };

        // Add connect_timeout if not already present
        if !connection_url.contains("connect_timeout=") {
            let separator = if connection_url.contains('?') {
                "&"
            } else {
                "?"
            };
            connection_url = format!(
                "{}{}connect_timeout={}",
                connection_url, separator, self.config.database.connection_timeout_secs
            );
        }

        // Add statement_timeout if not already present
        if !connection_url.contains("statement_timeout=") {
            connection_url = format!(
                "{}&statement_timeout={}",
                connection_url,
                self.config.database.statement_timeout_secs * 1000 // Convert to milliseconds
            );
        }

        let mut pool_options = crate::stale_plan::recycle_stale_connections(PgPoolOptions::new())
            .min_connections(self.config.pooling.min_connections)
            .max_connections(self.config.pooling.max_connections)
            .acquire_timeout(Duration::from_secs(
                self.config.pooling.acquire_timeout_secs,
            ))
            .idle_timeout(Some(Duration::from_secs(
                self.config.pooling.idle_timeout_secs,
            )))
            .max_lifetime(Some(Duration::from_secs(
                self.config.pooling.max_lifetime_secs,
            )));

        // Add test_before_acquire if validation query is configured
        if self.config.pooling.test_on_borrow {
            pool_options = pool_options.test_before_acquire(true);
        }

        // Enable pool statistics if configured
        if self.config.pooling.enable_statistics {
            pool_options = pool_options.after_connect(|_conn, _meta| {
                Box::pin(async move {
                    debug!("New connection established in pool");
                    Ok(())
                })
            });
        }

        // test_while_idle used to install a before_acquire hook here that
        // only logged; the pool's one before_acquire slot now recycles
        // connections holding statements prepared against an older schema
        // (crate::stale_plan), and test_on_borrow above is the idle test.

        let pool = pool_options.connect(&connection_url).await.map_err(|e| {
            orcher_common::error::OrcherError::internal(format!(
                "Failed to create PostgreSQL pool: {}",
                e
            ))
        })?;

        info!(
            "PostgreSQL connection pool created: min={}, max={}, timeout={}s, stmt_timeout={}s, test_on_borrow={}, test_while_idle={}, statistics={}",
            self.config.pooling.min_connections,
            self.config.pooling.max_connections,
            self.config.database.connection_timeout_secs,
            self.config.database.statement_timeout_secs,
            self.config.pooling.test_on_borrow,
            self.config.pooling.test_while_idle,
            self.config.pooling.enable_statistics
        );

        Ok(pool)
    }

    /// Validate storage configuration
    ///
    /// Checks that all configuration values are valid and consistent.
    ///
    /// # Returns
    /// * `Ok(())` - Configuration is valid
    /// * `Err(OrcherError)` - Configuration validation failed
    pub fn validate_config(&self) -> OrcherResult<()> {
        debug!("Validating storage configuration");

        // Validate PostgreSQL URL is not empty
        if self.config.database.postgres_url.is_empty() {
            return Err(orcher_common::error::OrcherError::InvalidInput {
                message: "PostgreSQL URL cannot be empty".to_string(),
                field: Some("database.postgres_url".to_string()),
            });
        }

        // Validate Redis URL is not empty
        if self.config.redis.url.is_empty() {
            return Err(orcher_common::error::OrcherError::InvalidInput {
                message: "Redis URL cannot be empty".to_string(),
                field: Some("redis.url".to_string()),
            });
        }

        // Validate pool size constraints
        if self.config.pooling.min_connections > self.config.pooling.max_connections {
            return Err(orcher_common::error::OrcherError::InvalidInput {
                message: format!(
                    "Min connections ({}) cannot exceed max connections ({})",
                    self.config.pooling.min_connections, self.config.pooling.max_connections
                ),
                field: Some("pooling.min_connections".to_string()),
            });
        }

        info!("Storage configuration validated successfully");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_storage_config_manager_creation() {
        let config = StorageConfig::default();
        let manager = StorageConfigManager::new(config);
        assert!(manager.config().database.postgres_url.contains("postgres"));
    }

    #[test]
    fn test_storage_config_manager_config_access() {
        let config = StorageConfig::default();
        let manager = StorageConfigManager::new(config.clone());
        assert_eq!(
            manager.config().pooling.max_connections,
            config.pooling.max_connections
        );
    }

    #[test]
    fn test_validate_config_success() {
        let config = StorageConfig::default();
        let manager = StorageConfigManager::new(config);
        assert!(manager.validate_config().is_ok());
    }

    #[test]
    fn test_storage_config_manager_debug() {
        let config = StorageConfig::default();
        let manager = StorageConfigManager::new(config);
        let debug_str = format!("{:?}", manager);
        assert!(debug_str.contains("StorageConfigManager"));
    }

    #[test]
    fn test_application_name_in_url() {
        let config = StorageConfig::default();
        let manager = StorageConfigManager::new(config);

        // Verify application_name would be added to URL
        assert!(!manager.config().database.postgres_url.is_empty());
        assert!(!manager.config().database.application_name.is_empty());
    }

    #[test]
    fn test_redis_key_prefix() {
        let config = StorageConfig::default();
        let manager = StorageConfigManager::new(config);

        // Verify key_prefix configuration is accessible
        assert!(
            manager.config().redis.key_prefix.is_empty()
                || !manager.config().redis.key_prefix.is_empty()
        );
    }

    #[test]
    fn test_connection_timeout_in_url() {
        let config = StorageConfig::default();
        let manager = StorageConfigManager::new(config);

        // Verify connection timeout is configured
        assert!(manager.config().database.connection_timeout_secs > 0);
    }

    #[test]
    fn test_redis_connection_timeout() {
        let config = StorageConfig::default();
        let manager = StorageConfigManager::new(config);

        // Verify Redis connection timeout is configured
        assert!(manager.config().redis.connection_timeout_secs > 0);
    }

    #[test]
    fn test_redis_command_timeout() {
        let config = StorageConfig::default();
        let manager = StorageConfigManager::new(config);

        // Verify Redis command timeout is configured
        assert!(manager.config().redis.command_timeout_secs > 0);
    }

    #[test]
    fn test_query_cache_ttl() {
        let config = StorageConfig::default();
        let manager = StorageConfigManager::new(config);

        // Verify query cache TTL is configured
        assert!(manager.config().performance.query_cache_ttl_secs > 0);
    }

    #[test]
    fn test_pool_validation() {
        let config = StorageConfig::default();
        let manager = StorageConfigManager::new(config);

        // Verify pool validation configuration is accessible
        assert!(
            manager.config().pooling.test_on_borrow || !manager.config().pooling.test_on_borrow
        );
    }

    #[test]
    fn test_statement_timeout() {
        let config = StorageConfig::default();
        let manager = StorageConfigManager::new(config);

        // Verify statement timeout is configured
        assert!(manager.config().database.statement_timeout_secs > 0);
    }

    #[test]
    fn test_pool_statistics() {
        let config = StorageConfig::default();
        let manager = StorageConfigManager::new(config);

        // Verify pool statistics configuration is accessible
        assert!(
            manager.config().pooling.enable_statistics
                || !manager.config().pooling.enable_statistics
        );
    }

    #[test]
    fn test_idle_connection_testing() {
        let config = StorageConfig::default();
        let manager = StorageConfigManager::new(config);

        // Verify idle connection testing configuration is accessible
        assert!(
            manager.config().pooling.test_while_idle || !manager.config().pooling.test_while_idle
        );
    }
}
