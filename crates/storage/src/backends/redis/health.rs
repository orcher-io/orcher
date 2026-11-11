//! # Redis Health Monitoring
//!
//! Health check implementation for Redis backend using orcher-common monitoring patterns.

use orcher_common::OrcherResult;
use redis::Client;
use std::time::Duration;
use tracing::{debug, warn};

/// Health status for Redis backend
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
    /// Redis server version
    pub server_version: Option<String>,
    /// Number of connected clients
    pub connected_clients: Option<u32>,
    /// Error message if unhealthy
    pub error_message: Option<String>,
}

/// Perform a basic health check on the Redis client
pub async fn check_health(client: &Client) -> OrcherResult<HealthCheckResult> {
    let start = std::time::Instant::now();

    // Get a connection and execute PING command
    let mut conn = match client.get_multiplexed_async_connection().await {
        Ok(c) => c,
        Err(e) => {
            let response_time_ms = start.elapsed().as_millis() as u64;
            warn!("Redis health check failed to connect: {}", e);
            return Ok(HealthCheckResult {
                status: HealthStatus::Unhealthy,
                response_time_ms,
                server_version: None,
                connected_clients: None,
                error_message: Some(e.to_string()),
            });
        }
    };

    let ping_result: Result<String, redis::RedisError> =
        redis::cmd("PING").query_async(&mut conn).await;

    let response_time_ms = start.elapsed().as_millis() as u64;

    match ping_result {
        Ok(_) => {
            debug!("Redis health check passed in {}ms", response_time_ms);

            let status = if response_time_ms > 500 {
                warn!("Redis response time is high: {}ms", response_time_ms);
                HealthStatus::Degraded
            } else {
                HealthStatus::Healthy
            };

            Ok(HealthCheckResult {
                status,
                response_time_ms,
                server_version: None,
                connected_clients: None,
                error_message: None,
            })
        }
        Err(e) => {
            warn!("Redis health check failed: {}", e);
            Ok(HealthCheckResult {
                status: HealthStatus::Unhealthy,
                response_time_ms,
                server_version: None,
                connected_clients: None,
                error_message: Some(e.to_string()),
            })
        }
    }
}

/// Perform a detailed health check with timeout
pub async fn check_health_with_timeout(
    client: &Client,
    timeout: Duration,
) -> OrcherResult<HealthCheckResult> {
    tokio::time::timeout(timeout, check_health(client))
        .await
        .map_err(|_| {
            orcher_common::error::OrcherError::internal(format!(
                "Redis health check timed out after {:?}",
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
            response_time_ms: 25,
            server_version: Some("7.0.0".to_string()),
            connected_clients: Some(10),
            error_message: None,
        };

        assert_eq!(result.status, HealthStatus::Healthy);
        assert_eq!(result.response_time_ms, 25);
        assert_eq!(result.server_version, Some("7.0.0".to_string()));
        assert!(result.error_message.is_none());
    }

    #[test]
    fn test_unhealthy_with_error() {
        let result = HealthCheckResult {
            status: HealthStatus::Unhealthy,
            response_time_ms: 5000,
            server_version: None,
            connected_clients: None,
            error_message: Some("Connection refused".to_string()),
        };

        assert_eq!(result.status, HealthStatus::Unhealthy);
        assert!(result.error_message.is_some());
        assert!(result.server_version.is_none());
    }
}
