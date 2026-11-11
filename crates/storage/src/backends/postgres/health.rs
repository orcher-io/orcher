//! # PostgreSQL Health Monitoring
//!
//! Health check implementation for PostgreSQL backend using orcher-common monitoring patterns.

use orcher_common::OrcherResult;
use sqlx::PgPool;
use std::time::Duration;
use tracing::{debug, warn};

/// Health status for PostgreSQL backend
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HealthStatus {
    /// Backend is healthy and operational
    Healthy,
    /// Backend is degraded but operational
    Degraded,
    /// Backend is unhealthy
    Unhealthy,
}

/// Health check result with details
#[derive(Debug, Clone)]
pub struct HealthCheckResult {
    /// Overall health status
    pub status: HealthStatus,
    /// Response time in milliseconds
    pub response_time_ms: u64,
    /// Number of active connections
    pub active_connections: u32,
    /// Number of idle connections
    pub idle_connections: u32,
    /// Error message if unhealthy
    pub error_message: Option<String>,
}

/// Perform a basic health check on the PostgreSQL connection pool
pub async fn check_health(pool: &PgPool) -> OrcherResult<HealthCheckResult> {
    let start = std::time::Instant::now();

    // Execute a simple query to verify connectivity
    let query_result = sqlx::query("SELECT 1").fetch_one(pool).await;

    let response_time_ms = start.elapsed().as_millis() as u64;

    match query_result {
        Ok(_) => {
            debug!("PostgreSQL health check passed in {}ms", response_time_ms);

            let status = if response_time_ms > 1000 {
                warn!("PostgreSQL response time is high: {}ms", response_time_ms);
                HealthStatus::Degraded
            } else {
                HealthStatus::Healthy
            };

            Ok(HealthCheckResult {
                status,
                response_time_ms,
                active_connections: pool.size(),
                idle_connections: pool.num_idle() as u32,
                error_message: None,
            })
        }
        Err(e) => {
            warn!("PostgreSQL health check failed: {}", e);
            Ok(HealthCheckResult {
                status: HealthStatus::Unhealthy,
                response_time_ms,
                active_connections: pool.size(),
                idle_connections: pool.num_idle() as u32,
                error_message: Some(e.to_string()),
            })
        }
    }
}

/// Perform a detailed health check with timeout
pub async fn check_health_with_timeout(
    pool: &PgPool,
    timeout: Duration,
) -> OrcherResult<HealthCheckResult> {
    tokio::time::timeout(timeout, check_health(pool))
        .await
        .map_err(|_| {
            orcher_common::error::OrcherError::internal(format!(
                "PostgreSQL health check timed out after {:?}",
                timeout
            ))
        })?
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_health_status_equality() {
        assert_eq!(HealthStatus::Healthy, HealthStatus::Healthy);
        assert_eq!(HealthStatus::Degraded, HealthStatus::Degraded);
        assert_eq!(HealthStatus::Unhealthy, HealthStatus::Unhealthy);
        assert_ne!(HealthStatus::Healthy, HealthStatus::Degraded);
    }

    #[test]
    fn test_health_check_result_creation() {
        let result = HealthCheckResult {
            status: HealthStatus::Healthy,
            response_time_ms: 50,
            active_connections: 5,
            idle_connections: 3,
            error_message: None,
        };

        assert_eq!(result.status, HealthStatus::Healthy);
        assert_eq!(result.response_time_ms, 50);
        assert_eq!(result.active_connections, 5);
        assert!(result.error_message.is_none());
    }

    #[test]
    fn test_unhealthy_with_error() {
        let result = HealthCheckResult {
            status: HealthStatus::Unhealthy,
            response_time_ms: 1500,
            active_connections: 0,
            idle_connections: 0,
            error_message: Some("Connection refused".to_string()),
        };

        assert_eq!(result.status, HealthStatus::Unhealthy);
        assert!(result.error_message.is_some());
    }
}
