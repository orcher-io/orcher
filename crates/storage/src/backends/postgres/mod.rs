//! # PostgreSQL Backend Module
//!
//! PostgreSQL backend implementation with connection pooling, transactions,
//! health monitoring, and connection pool metrics using orcher-common utilities.
//!
//! ## Connection Pool Monitoring
//!
//! The `PostgresBackend` provides methods for monitoring connection pool health
//! and metrics, which can be integrated with Prometheus via `orcher-monitoring`.
//!
//! ```rust,ignore
//! use orcher_storage::backends::postgres::PostgresBackend;
//!
//! let backend = PostgresBackend::new(config).await?;
//!
//! // Get pool metrics for monitoring
//! let metrics = backend.pool_metrics();
//! println!("Active: {}, Idle: {}, Max: {}", metrics.active, metrics.idle, metrics.max);
//!
//! // Start background monitoring (optional)
//! let monitor = backend.create_pool_monitor();
//! let handle = monitor.start(std::time::Duration::from_secs(5));
//! ```

/// Connection management and pooling
pub mod connection;

/// Health monitoring and checks
pub mod health;

/// Transaction management
pub mod transactions;

/// Repository implementations
pub mod repository;

// Re-export main types for convenience
pub use connection::{PostgresConfig, PostgresConnection};
pub use health::{HealthCheckResult, HealthStatus};
pub use repository::BaseRepository;
pub use transactions::{begin_transaction, PgTransaction};

/// Connection pool metrics snapshot
#[derive(Debug, Clone, Default)]
pub struct PoolMetrics {
    /// Number of connections currently in use
    pub active: u32,
    /// Number of idle connections available
    pub idle: u32,
    /// Total connections in pool (active + idle)
    pub total: u32,
    /// Maximum connections configured
    pub max: u32,
}

impl PoolMetrics {
    /// Calculate utilization ratio (0.0 to 1.0)
    pub fn utilization_ratio(&self) -> f64 {
        if self.max == 0 {
            0.0
        } else {
            self.active as f64 / self.max as f64
        }
    }

    /// Check if pool is approaching exhaustion (>80% utilization)
    pub fn is_high_utilization(&self) -> bool {
        self.utilization_ratio() > 0.8
    }

    /// Check if pool is critically low (<10% available)
    pub fn is_critical(&self) -> bool {
        self.utilization_ratio() > 0.9
    }
}

/// PostgreSQL backend with connection pool and health monitoring
#[derive(Debug, Clone)]
pub struct PostgresBackend {
    pool: sqlx::PgPool,
}

impl PostgresBackend {
    /// Create a new PostgreSQL backend with the given configuration
    pub async fn new(config: PostgresConfig) -> orcher_common::OrcherResult<Self> {
        let pool = connection::create_pool(&config).await?;
        Ok(Self { pool })
    }

    /// Get a reference to the connection pool
    pub fn pool(&self) -> &sqlx::PgPool {
        &self.pool
    }

    /// Close the connection pool
    pub async fn close(&self) {
        self.pool.close().await;
    }

    /// Perform a health check on the backend
    pub async fn health_check(&self) -> orcher_common::OrcherResult<HealthCheckResult> {
        health::check_health(&self.pool).await
    }

    /// Perform a health check with timeout
    pub async fn health_check_with_timeout(
        &self,
        timeout: std::time::Duration,
    ) -> orcher_common::OrcherResult<HealthCheckResult> {
        health::check_health_with_timeout(&self.pool, timeout).await
    }

    /// Get current connection pool metrics
    ///
    /// Returns a snapshot of the current pool state including active,
    /// idle, and total connection counts.
    pub fn pool_metrics(&self) -> PoolMetrics {
        let total = self.pool.size();
        let idle = self.pool.num_idle() as u32;
        let active = total.saturating_sub(idle);
        let max = self.pool.options().get_max_connections();

        PoolMetrics {
            active,
            idle,
            total,
            max,
        }
    }

    /// Get current pool size (total connections)
    pub fn pool_size(&self) -> u32 {
        self.pool.size()
    }

    /// Get number of idle connections
    pub fn pool_idle(&self) -> u32 {
        self.pool.num_idle() as u32
    }

    /// Get number of active (in-use) connections
    pub fn pool_active(&self) -> u32 {
        self.pool.size().saturating_sub(self.pool.num_idle() as u32)
    }

    /// Get maximum configured connections
    pub fn pool_max_connections(&self) -> u32 {
        self.pool.options().get_max_connections()
    }

    /// Check if the pool is approaching exhaustion
    ///
    /// Returns true if more than 80% of connections are in use.
    pub fn is_pool_high_utilization(&self) -> bool {
        self.pool_metrics().is_high_utilization()
    }

    /// Check if the pool is critically exhausted
    ///
    /// Returns true if more than 90% of connections are in use.
    pub fn is_pool_critical(&self) -> bool {
        self.pool_metrics().is_critical()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_postgres_backend_creation() {
        // Compile-time test to ensure types are properly defined
        fn _assert_backend_traits<T: Clone + std::fmt::Debug>() {}
        _assert_backend_traits::<PostgresBackend>();
    }

    #[test]
    fn test_pool_metrics_utilization() {
        let metrics = PoolMetrics {
            active: 5,
            idle: 3,
            total: 8,
            max: 20,
        };

        assert!((metrics.utilization_ratio() - 0.25).abs() < 0.001);
        assert!(!metrics.is_high_utilization());
        assert!(!metrics.is_critical());
    }

    #[test]
    fn test_pool_metrics_high_utilization() {
        let metrics = PoolMetrics {
            active: 17,
            idle: 1,
            total: 18,
            max: 20,
        };

        assert!(metrics.is_high_utilization());
        assert!(!metrics.is_critical());
    }

    #[test]
    fn test_pool_metrics_critical() {
        let metrics = PoolMetrics {
            active: 19,
            idle: 0,
            total: 19,
            max: 20,
        };

        assert!(metrics.is_high_utilization());
        assert!(metrics.is_critical());
    }

    #[test]
    fn test_pool_metrics_zero_max() {
        let metrics = PoolMetrics {
            active: 0,
            idle: 0,
            total: 0,
            max: 0,
        };

        assert_eq!(metrics.utilization_ratio(), 0.0);
        assert!(!metrics.is_high_utilization());
        assert!(!metrics.is_critical());
    }
}
