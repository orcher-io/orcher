//! HTTP endpoints for health check system
//!
//! This module provides HTTP endpoints for exposing health check results
//! to external monitoring systems, load balancers, and service discovery.

use crate::error::Result;
use crate::health::registry::{AggregatedHealthStatus, HealthCheckRegistry};
use orcher_common::monitoring::HealthStatus;
use serde_json::json;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

/// HTTP status codes for health check responses
pub mod status_codes {
    /// OK - System is healthy
    pub const OK: u16 = 200;
    /// Service Unavailable - System is unhealthy
    pub const SERVICE_UNAVAILABLE: u16 = 503;
    /// Internal Server Error - Health check failed
    pub const INTERNAL_SERVER_ERROR: u16 = 500;
}

/// Health check endpoint configuration
#[derive(Debug, Clone)]
pub struct HealthEndpointConfig {
    /// Whether to include detailed check results in responses
    pub include_details: bool,
    /// Whether to include timing information
    pub include_timing: bool,
    /// Whether to include check metadata
    pub include_metadata: bool,
    /// Custom headers to include in responses
    pub custom_headers: HashMap<String, String>,
}

impl Default for HealthEndpointConfig {
    fn default() -> Self {
        Self {
            include_details: true,
            include_timing: true,
            include_metadata: false,
            custom_headers: HashMap::new(),
        }
    }
}

/// HTTP response for health checks
#[derive(Debug, Clone)]
pub struct HealthResponse {
    /// HTTP status code
    pub status_code: u16,
    /// Response headers
    pub headers: HashMap<String, String>,
    /// Response body (JSON)
    pub body: String,
}

impl HealthResponse {
    /// Create a new health response
    pub fn new(status_code: u16, body: String) -> Self {
        let mut headers = HashMap::new();
        headers.insert("Content-Type".to_string(), "application/json".to_string());
        headers.insert("Cache-Control".to_string(), "no-cache".to_string());

        Self {
            status_code,
            headers,
            body,
        }
    }

    /// Add a header to the response
    pub fn with_header(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.insert(key.into(), value.into());
        self
    }

    /// Add multiple headers to the response
    pub fn with_headers(mut self, headers: HashMap<String, String>) -> Self {
        self.headers.extend(headers);
        self
    }
}

/// Health check endpoints handler
pub struct HealthEndpoints {
    /// Health check registry
    registry: Arc<HealthCheckRegistry>,
    /// Endpoint configuration
    config: HealthEndpointConfig,
}

impl HealthEndpoints {
    /// Create new health endpoints
    pub fn new(registry: Arc<HealthCheckRegistry>) -> Self {
        Self {
            registry,
            config: HealthEndpointConfig::default(),
        }
    }

    /// Create new health endpoints with custom configuration
    pub fn with_config(registry: Arc<HealthCheckRegistry>, config: HealthEndpointConfig) -> Self {
        Self { registry, config }
    }

    /// Health check endpoint (GET /health)
    /// Returns 200 if healthy, 503 if unhealthy
    pub async fn health(&self) -> Result<HealthResponse> {
        let status = self.registry.check_all().await?;
        let status_code = match status.status {
            HealthStatus::Healthy => status_codes::OK,
            HealthStatus::Unhealthy => status_codes::SERVICE_UNAVAILABLE,
            HealthStatus::Degraded => status_codes::SERVICE_UNAVAILABLE,
            HealthStatus::Unknown => status_codes::SERVICE_UNAVAILABLE,
        };

        let response_body = self.format_health_response(&status)?;
        let mut response = HealthResponse::new(status_code, response_body);

        // Add custom headers
        response = response.with_headers(self.config.custom_headers.clone());

        Ok(response)
    }

    /// Readiness check endpoint (GET /ready)
    /// Returns 200 if ready to serve traffic, 503 if not ready
    pub async fn ready(&self) -> Result<HealthResponse> {
        let status = self.registry.check_all().await?;

        // System is ready if all critical checks pass
        let is_ready = status.summary.critical_checks == 0
            || status.summary.healthy_critical == status.summary.critical_checks;

        let status_code = if is_ready {
            status_codes::OK
        } else {
            status_codes::SERVICE_UNAVAILABLE
        };

        let body = json!({
            "status": if is_ready { "ready" } else { "not_ready" },
            "timestamp": current_timestamp(),
            "checks": {
                "critical": {
                    "total": status.summary.critical_checks,
                    "healthy": status.summary.healthy_critical
                }
            }
        });

        let mut response = HealthResponse::new(status_code, body.to_string());
        response = response.with_headers(self.config.custom_headers.clone());

        Ok(response)
    }

    /// Liveness check endpoint (GET /live)
    /// Returns 200 if process is alive, 503 if should be restarted
    pub async fn live(&self) -> Result<HealthResponse> {
        // For liveness, we just check if the registry is responsive
        // If we can get cached results, the system is alive
        let cached_results = self.registry.get_cached_results();

        let body = json!({
            "status": "alive",
            "timestamp": current_timestamp(),
            "cached_checks": cached_results.len()
        });

        let mut response = HealthResponse::new(status_codes::OK, body.to_string());
        response = response.with_headers(self.config.custom_headers.clone());

        Ok(response)
    }

    /// Detailed health status endpoint (GET /health/details)
    /// Returns detailed information about all health checks
    pub async fn health_details(&self) -> Result<HealthResponse> {
        let status = self.registry.check_all().await?;
        let status_code = match status.status {
            HealthStatus::Healthy => status_codes::OK,
            HealthStatus::Unhealthy => status_codes::SERVICE_UNAVAILABLE,
            HealthStatus::Degraded => status_codes::SERVICE_UNAVAILABLE,
            HealthStatus::Unknown => status_codes::SERVICE_UNAVAILABLE,
        };

        let response_body = self.format_detailed_response(&status)?;
        let mut response = HealthResponse::new(status_code, response_body);
        response = response.with_headers(self.config.custom_headers.clone());

        Ok(response)
    }

    /// Individual health check endpoint (GET /health/check/{name})
    /// Returns status of a specific health check
    pub async fn health_check(&self, check_name: &str) -> Result<HealthResponse> {
        match self.registry.check_one(check_name).await? {
            Some(result) => {
                let status_code = if result.is_healthy() {
                    status_codes::OK
                } else {
                    status_codes::SERVICE_UNAVAILABLE
                };

                let body = json!({
                    "name": result.component_name,
                    "status": format!("{:?}", result.status).to_lowercase(),
                    "message": result.message,
                    "duration_ms": result.duration.as_millis(),
                    "timestamp": current_timestamp(),
                    "metadata": if self.config.include_metadata { result.metadata } else { HashMap::new() }
                });

                let mut response = HealthResponse::new(status_code, body.to_string());
                response = response.with_headers(self.config.custom_headers.clone());
                Ok(response)
            }
            None => {
                let body = json!({
                    "error": "Health check not found",
                    "check_name": check_name,
                    "timestamp": current_timestamp()
                });

                let response = HealthResponse::new(404, body.to_string());
                Ok(response)
            }
        }
    }

    /// List all registered health checks (GET /health/checks)
    pub async fn list_checks(&self) -> Result<HealthResponse> {
        let check_names = self.registry.list_checks();

        let body = json!({
            "checks": check_names,
            "count": check_names.len(),
            "timestamp": current_timestamp()
        });

        let mut response = HealthResponse::new(status_codes::OK, body.to_string());
        response = response.with_headers(self.config.custom_headers.clone());

        Ok(response)
    }

    /// Format basic health response
    fn format_health_response(&self, status: &AggregatedHealthStatus) -> Result<String> {
        let mut response = json!({
            "status": format!("{:?}", status.status).to_lowercase(),
            "timestamp": current_timestamp(),
            "summary": {
                "total_checks": status.summary.total_checks,
                "healthy_checks": status.summary.healthy_total,
                "critical": {
                    "total": status.summary.critical_checks,
                    "healthy": status.summary.healthy_critical
                },
                "important": {
                    "total": status.summary.important_checks,
                    "healthy": status.summary.healthy_important
                },
                "informational": {
                    "total": status.summary.informational_checks,
                    "healthy": status.summary.healthy_informational
                }
            }
        });

        if self.config.include_timing {
            response["timing"] = json!({
                "total_duration_ms": status.total_duration.as_millis()
            });
        }

        if self.config.include_details {
            let checks: HashMap<String, serde_json::Value> = status
                .checks
                .iter()
                .map(|(name, result)| {
                    let check_data = json!({
                        "status": format!("{:?}", result.status).to_lowercase(),
                        "message": result.message,
                        "duration_ms": result.duration.as_millis()
                    });
                    (name.clone(), check_data)
                })
                .collect();

            response["checks"] = json!(checks);
        }

        Ok(response.to_string())
    }

    /// Format detailed health response
    fn format_detailed_response(&self, status: &AggregatedHealthStatus) -> Result<String> {
        let checks: HashMap<String, serde_json::Value> = status
            .checks
            .iter()
            .map(|(name, result)| {
                let mut check_data = json!({
                    "status": format!("{:?}", result.status).to_lowercase(),
                    "message": result.message,
                    "duration_ms": result.duration.as_millis(),
                    "timestamp": current_timestamp()
                });

                if self.config.include_metadata && !result.metadata.is_empty() {
                    check_data["metadata"] = json!(result.metadata);
                }

                (name.clone(), check_data)
            })
            .collect();

        let response = json!({
            "status": format!("{:?}", status.status).to_lowercase(),
            "timestamp": current_timestamp(),
            "summary": {
                "total_checks": status.summary.total_checks,
                "healthy_checks": status.summary.healthy_total,
                "critical": {
                    "total": status.summary.critical_checks,
                    "healthy": status.summary.healthy_critical
                },
                "important": {
                    "total": status.summary.important_checks,
                    "healthy": status.summary.healthy_important
                },
                "informational": {
                    "total": status.summary.informational_checks,
                    "healthy": status.summary.healthy_informational
                }
            },
            "timing": {
                "total_duration_ms": status.total_duration.as_millis()
            },
            "checks": checks
        });

        Ok(response.to_string())
    }
}

/// Get current timestamp as seconds since epoch
fn current_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::health::check::{AsyncHealthCheck, HealthCheckCriticality, HealthCheckResult};
    use crate::health::registry::HealthCheckRegistry;
    use orcher_common::monitoring::HealthStatus;

    /// Mock health check for testing
    struct MockHealthCheck {
        name: String,
        status: HealthStatus,
        message: String,
    }

    impl MockHealthCheck {
        fn new(name: &str, status: HealthStatus, message: &str) -> Self {
            Self {
                name: name.to_string(),
                status,
                message: message.to_string(),
            }
        }
    }

    #[async_trait::async_trait]
    impl AsyncHealthCheck for MockHealthCheck {
        fn name(&self) -> &str {
            &self.name
        }

        async fn check(&self) -> Result<HealthCheckResult> {
            Ok(HealthCheckResult::new(&self.name, self.status).with_message(&self.message))
        }
    }

    #[tokio::test]
    async fn test_health_endpoint_healthy() {
        let registry = Arc::new(HealthCheckRegistry::new());
        let check = Arc::new(MockHealthCheck::new("test", HealthStatus::Healthy, "OK"));
        registry.register(check).unwrap();

        let endpoints = HealthEndpoints::new(registry);
        let response = endpoints.health().await.unwrap();

        assert_eq!(response.status_code, status_codes::OK);
        assert!(response.body.contains("healthy"));
        assert_eq!(
            response.headers.get("Content-Type"),
            Some(&"application/json".to_string())
        );
    }

    #[tokio::test]
    async fn test_health_endpoint_unhealthy() {
        let registry = Arc::new(HealthCheckRegistry::new());
        let check = Arc::new(MockHealthCheck::new(
            "test",
            HealthStatus::Unhealthy,
            "Failed",
        ));
        registry
            .register_with_criticality(check, HealthCheckCriticality::Critical)
            .unwrap();

        let endpoints = HealthEndpoints::new(registry);
        let response = endpoints.health().await.unwrap();

        assert_eq!(response.status_code, status_codes::SERVICE_UNAVAILABLE);
        assert!(response.body.contains("unhealthy"));
    }

    #[tokio::test]
    async fn test_ready_endpoint() {
        let registry = Arc::new(HealthCheckRegistry::new());
        let check = Arc::new(MockHealthCheck::new(
            "critical",
            HealthStatus::Healthy,
            "OK",
        ));
        registry
            .register_with_criticality(check, HealthCheckCriticality::Critical)
            .unwrap();

        let endpoints = HealthEndpoints::new(registry);
        let response = endpoints.ready().await.unwrap();

        assert_eq!(response.status_code, status_codes::OK);
        assert!(response.body.contains("ready"));
    }

    #[tokio::test]
    async fn test_ready_endpoint_not_ready() {
        let registry = Arc::new(HealthCheckRegistry::new());
        let check = Arc::new(MockHealthCheck::new(
            "critical",
            HealthStatus::Unhealthy,
            "Failed",
        ));
        registry
            .register_with_criticality(check, HealthCheckCriticality::Critical)
            .unwrap();

        let endpoints = HealthEndpoints::new(registry);
        let response = endpoints.ready().await.unwrap();

        assert_eq!(response.status_code, status_codes::SERVICE_UNAVAILABLE);
        assert!(response.body.contains("not_ready"));
    }

    #[tokio::test]
    async fn test_live_endpoint() {
        let registry = Arc::new(HealthCheckRegistry::new());
        let endpoints = HealthEndpoints::new(registry);
        let response = endpoints.live().await.unwrap();

        assert_eq!(response.status_code, status_codes::OK);
        assert!(response.body.contains("alive"));
    }

    #[tokio::test]
    async fn test_health_details_endpoint() {
        let registry = Arc::new(HealthCheckRegistry::new());
        let check = Arc::new(MockHealthCheck::new(
            "detailed",
            HealthStatus::Healthy,
            "OK",
        ));
        registry.register(check).unwrap();

        let endpoints = HealthEndpoints::new(registry);
        let response = endpoints.health_details().await.unwrap();

        assert_eq!(response.status_code, status_codes::OK);
        assert!(response.body.contains("detailed"));
        assert!(response.body.contains("timing"));
    }

    #[tokio::test]
    async fn test_individual_health_check_endpoint() {
        let registry = Arc::new(HealthCheckRegistry::new());
        let check = Arc::new(MockHealthCheck::new(
            "individual",
            HealthStatus::Healthy,
            "OK",
        ));
        registry.register(check).unwrap();

        let endpoints = HealthEndpoints::new(registry);
        let response = endpoints.health_check("individual").await.unwrap();

        assert_eq!(response.status_code, status_codes::OK);
        assert!(response.body.contains("individual"));
    }

    #[tokio::test]
    async fn test_individual_health_check_not_found() {
        let registry = Arc::new(HealthCheckRegistry::new());
        let endpoints = HealthEndpoints::new(registry);
        let response = endpoints.health_check("nonexistent").await.unwrap();

        assert_eq!(response.status_code, 404);
        assert!(response.body.contains("not found"));
    }

    #[tokio::test]
    async fn test_list_checks_endpoint() {
        let registry = Arc::new(HealthCheckRegistry::new());
        let check1 = Arc::new(MockHealthCheck::new("check1", HealthStatus::Healthy, "OK"));
        let check2 = Arc::new(MockHealthCheck::new("check2", HealthStatus::Healthy, "OK"));
        registry.register(check1).unwrap();
        registry.register(check2).unwrap();

        let endpoints = HealthEndpoints::new(registry);
        let response = endpoints.list_checks().await.unwrap();

        assert_eq!(response.status_code, status_codes::OK);
        assert!(response.body.contains("check1"));
        assert!(response.body.contains("check2"));
        assert!(response.body.contains("\"count\":2"));
    }

    #[tokio::test]
    async fn test_custom_configuration() {
        let registry = Arc::new(HealthCheckRegistry::new());
        let check = Arc::new(MockHealthCheck::new("custom", HealthStatus::Healthy, "OK"));
        registry.register(check).unwrap();

        let mut custom_headers = HashMap::new();
        custom_headers.insert("X-Service".to_string(), "ORCHER".to_string());

        let config = HealthEndpointConfig {
            include_details: false,
            include_timing: false,
            include_metadata: true,
            custom_headers,
        };

        let endpoints = HealthEndpoints::with_config(registry, config);
        let response = endpoints.health().await.unwrap();

        assert_eq!(response.status_code, status_codes::OK);
        assert_eq!(
            response.headers.get("X-Service"),
            Some(&"ORCHER".to_string())
        );
        // Should not include details or timing due to config
        assert!(!response.body.contains("\"checks\":"));
        assert!(!response.body.contains("\"timing\":"));
    }
}
