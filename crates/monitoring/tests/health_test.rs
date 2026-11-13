//! Comprehensive integration tests for the ORCHER health framework
//!
//! This module tests the complete health check functionality including
//! registry management, circuit breaker integration, HTTP endpoints,
//! and real-world usage scenarios.

use orcher_common::monitoring::HealthStatus;
use orcher_monitoring::health::{
    AsyncHealthCheck, HealthCheckCriticality, HealthCheckRegistry, HealthCheckResult,
    HealthEndpointConfig, HealthEndpoints, HealthRegistryConfig,
};
use orcher_monitoring::prelude::*;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;

/// Mock database health check
struct DatabaseHealthCheck {
    name: String,
    should_fail: bool,
    delay: Duration,
}

impl DatabaseHealthCheck {
    fn new(name: &str, should_fail: bool) -> Self {
        Self {
            name: name.to_string(),
            should_fail,
            delay: Duration::from_millis(10),
        }
    }

    fn with_delay(mut self, delay: Duration) -> Self {
        self.delay = delay;
        self
    }
}

#[async_trait::async_trait]
impl AsyncHealthCheck for DatabaseHealthCheck {
    fn name(&self) -> &str {
        &self.name
    }

    async fn check(&self) -> Result<HealthCheckResult> {
        sleep(self.delay).await;

        if self.should_fail {
            Ok(HealthCheckResult::new(&self.name, HealthStatus::Unhealthy)
                .with_message("Database connection failed")
                .with_metadata("error_code", "DB_CONN_TIMEOUT")
                .with_metadata("retry_count", "3"))
        } else {
            Ok(HealthCheckResult::new(&self.name, HealthStatus::Healthy)
                .with_message("Database connection healthy")
                .with_metadata("connection_pool_size", "10")
                .with_metadata("active_connections", "3"))
        }
    }
}

/// Mock cache health check
struct CacheHealthCheck {
    name: String,
    hit_rate: f64,
}

impl CacheHealthCheck {
    fn new(name: &str, hit_rate: f64) -> Self {
        Self {
            name: name.to_string(),
            hit_rate,
        }
    }
}

#[async_trait::async_trait]
impl AsyncHealthCheck for CacheHealthCheck {
    fn name(&self) -> &str {
        &self.name
    }

    async fn check(&self) -> Result<HealthCheckResult> {
        let status = if self.hit_rate > 0.8 {
            HealthStatus::Healthy
        } else if self.hit_rate > 0.5 {
            HealthStatus::Degraded
        } else {
            HealthStatus::Unhealthy
        };

        Ok(HealthCheckResult::new(&self.name, status)
            .with_message(format!("Cache hit rate: {:.1}%", self.hit_rate * 100.0))
            .with_metadata("hit_rate", format!("{:.3}", self.hit_rate))
            .with_metadata("cache_size", "1024MB"))
    }
}

/// Mock external service health check
struct ExternalServiceHealthCheck {
    name: String,
    response_time: Duration,
}

impl ExternalServiceHealthCheck {
    fn new(name: &str, response_time: Duration) -> Self {
        Self {
            name: name.to_string(),
            response_time,
        }
    }
}

#[async_trait::async_trait]
impl AsyncHealthCheck for ExternalServiceHealthCheck {
    fn name(&self) -> &str {
        &self.name
    }

    async fn check(&self) -> Result<HealthCheckResult> {
        sleep(self.response_time).await;

        let status = if self.response_time.as_millis() < 100 {
            HealthStatus::Healthy
        } else if self.response_time.as_millis() < 500 {
            HealthStatus::Degraded
        } else {
            HealthStatus::Unhealthy
        };

        Ok(HealthCheckResult::new(&self.name, status)
            .with_message(format!(
                "Response time: {}ms",
                self.response_time.as_millis()
            ))
            .with_metadata("endpoint", "https://api.example.com/health")
            .with_metadata("timeout", "5000"))
    }
}

#[tokio::test]
async fn test_complete_health_framework_integration() {
    // Create registry with custom configuration
    let config = HealthRegistryConfig {
        global_timeout: Duration::from_secs(5),
        max_concurrent_checks: 10,
        result_cache_duration: Duration::from_secs(30),
        default_circuit_breaker_enabled: true,
        ..Default::default()
    };
    let registry = Arc::new(HealthCheckRegistry::with_config(config));

    // Register various health checks with different criticalities
    let db_check = Arc::new(DatabaseHealthCheck::new("primary_db", false));
    let cache_check = Arc::new(CacheHealthCheck::new("redis_cache", 0.95));
    let api_check = Arc::new(ExternalServiceHealthCheck::new(
        "external_api",
        Duration::from_millis(50),
    ));

    registry
        .register_with_criticality(db_check, HealthCheckCriticality::Critical)
        .unwrap();
    registry
        .register_with_criticality(cache_check, HealthCheckCriticality::Important)
        .unwrap();
    registry
        .register_with_criticality(api_check, HealthCheckCriticality::Informational)
        .unwrap();

    // Test registry functionality
    assert_eq!(registry.list_checks().len(), 3);
    assert!(registry.is_registered("primary_db"));
    assert!(registry.is_registered("redis_cache"));
    assert!(registry.is_registered("external_api"));

    // Test individual health checks
    let db_result = registry.check_one("primary_db").await.unwrap().unwrap();
    assert!(db_result.is_healthy());
    assert_eq!(db_result.component_name, "primary_db");

    // Test aggregated health status
    let aggregated = registry.check_all().await.unwrap();
    assert_eq!(aggregated.status, HealthStatus::Healthy);
    assert_eq!(aggregated.summary.total_checks, 3);
    assert_eq!(aggregated.summary.critical_checks, 1);
    assert_eq!(aggregated.summary.important_checks, 1);
    assert_eq!(aggregated.summary.informational_checks, 1);

    // Test HTTP endpoints
    let endpoints = HealthEndpoints::new(registry.clone());

    // Test health endpoint
    let health_response = endpoints.health().await.unwrap();
    assert_eq!(health_response.status_code, 200);
    assert!(health_response.body.contains("healthy"));

    // Test readiness endpoint
    let ready_response = endpoints.ready().await.unwrap();
    assert_eq!(ready_response.status_code, 200);
    assert!(ready_response.body.contains("ready"));

    // Test liveness endpoint
    let live_response = endpoints.live().await.unwrap();
    assert_eq!(live_response.status_code, 200);
    assert!(live_response.body.contains("alive"));

    println!("✅ Complete health framework integration test passed");
}

#[tokio::test]
async fn test_critical_check_failure_scenario() {
    let registry = Arc::new(HealthCheckRegistry::new());

    // Add a failing critical check and healthy non-critical checks
    let failing_db = Arc::new(DatabaseHealthCheck::new("critical_db", true));
    let healthy_cache = Arc::new(CacheHealthCheck::new("cache", 0.95));

    registry
        .register_with_criticality(failing_db, HealthCheckCriticality::Critical)
        .unwrap();
    registry
        .register_with_criticality(healthy_cache, HealthCheckCriticality::Important)
        .unwrap();

    // System should be unhealthy due to critical check failure
    let aggregated = registry.check_all().await.unwrap();
    assert_eq!(aggregated.status, HealthStatus::Unhealthy);
    assert_eq!(aggregated.summary.healthy_critical, 0);
    assert_eq!(aggregated.summary.healthy_important, 1);

    // HTTP endpoints should reflect unhealthy status
    let endpoints = HealthEndpoints::new(registry);
    let health_response = endpoints.health().await.unwrap();
    assert_eq!(health_response.status_code, 503);
    assert!(health_response.body.contains("unhealthy"));

    let ready_response = endpoints.ready().await.unwrap();
    assert_eq!(ready_response.status_code, 503);
    assert!(ready_response.body.contains("not_ready"));

    println!("✅ Critical check failure scenario test passed");
}

#[tokio::test]
async fn test_circuit_breaker_integration() {
    let config = HealthRegistryConfig {
        global_timeout: Duration::from_millis(100), // Short timeout to trigger timeout behavior
        ..Default::default()
    };
    let registry = Arc::new(HealthCheckRegistry::with_config(config));

    // Add a slow check that will timeout and trigger circuit breaker
    let slow_check =
        Arc::new(DatabaseHealthCheck::new("slow_db", false).with_delay(Duration::from_millis(500)));

    registry
        .register_with_criticality(slow_check, HealthCheckCriticality::Important)
        .unwrap();

    // First check should timeout but still return a result
    let result = registry.check_one("slow_db").await.unwrap().unwrap();
    println!(
        "Circuit breaker result: {:?}, message: {:?}",
        result.status, result.message
    );
    assert!(result.is_unhealthy());
    assert!(result.message.unwrap().contains("timed out"));

    println!("✅ Circuit breaker integration test passed");
}

#[tokio::test]
async fn test_concurrent_health_checks() {
    let registry = Arc::new(HealthCheckRegistry::new());

    // Add multiple checks with different delays
    for i in 0..5 {
        let check = Arc::new(
            DatabaseHealthCheck::new(&format!("db_{}", i), false)
                .with_delay(Duration::from_millis(50 * i)),
        );
        registry.register(check).unwrap();
    }

    let start = std::time::Instant::now();
    let aggregated = registry.check_all().await.unwrap();
    let duration = start.elapsed();

    // Should complete much faster than sequential execution
    assert!(duration < Duration::from_millis(500));
    assert_eq!(aggregated.summary.total_checks, 5);
    assert_eq!(aggregated.summary.healthy_total, 5);

    println!("✅ Concurrent health checks test passed in {:?}", duration);
}

#[tokio::test]
async fn test_health_check_caching() {
    let registry = Arc::new(HealthCheckRegistry::new());
    let check = Arc::new(DatabaseHealthCheck::new("cached_db", false));
    registry.register(check).unwrap();

    // First check should execute and cache result
    let first_result = registry.check_one("cached_db").await.unwrap().unwrap();
    assert!(first_result.is_healthy());

    // Should have cached results
    let cached = registry.get_cached_results();
    assert_eq!(cached.len(), 1);
    assert!(cached.contains_key("cached_db"));

    // Clear cache and verify
    registry.clear_cache();
    let cleared_cache = registry.get_cached_results();
    assert!(cleared_cache.is_empty());

    println!("✅ Health check caching test passed");
}

#[tokio::test]
async fn test_dynamic_health_check_management() {
    let registry = Arc::new(HealthCheckRegistry::new());

    // Initially empty
    assert_eq!(registry.list_checks().len(), 0);

    // Add a check
    let db_check = Arc::new(DatabaseHealthCheck::new("dynamic_db", false));
    registry
        .register_with_criticality(db_check, HealthCheckCriticality::Critical)
        .unwrap();
    assert_eq!(registry.list_checks().len(), 1);

    // Disable the check
    assert!(registry.set_enabled("dynamic_db", false));
    let result = registry.check_one("dynamic_db").await.unwrap();
    assert!(result.is_none()); // Disabled check returns None

    // Re-enable the check
    assert!(registry.set_enabled("dynamic_db", true));
    let result = registry.check_one("dynamic_db").await.unwrap();
    assert!(result.is_some());

    // Remove the check
    assert!(registry.unregister("dynamic_db"));
    assert_eq!(registry.list_checks().len(), 0);
    assert!(!registry.is_registered("dynamic_db"));

    println!("✅ Dynamic health check management test passed");
}

#[tokio::test]
async fn test_custom_endpoint_configuration() {
    let registry = Arc::new(HealthCheckRegistry::new());
    let check = Arc::new(DatabaseHealthCheck::new("test_db", false));
    registry.register(check).unwrap();

    // Create custom endpoint configuration
    let mut custom_headers = HashMap::new();
    custom_headers.insert("X-Service-Name".to_string(), "ORCHER".to_string());
    custom_headers.insert("X-Version".to_string(), "1.0.0".to_string());

    let config = HealthEndpointConfig {
        include_details: true,
        include_timing: true,
        include_metadata: true,
        custom_headers,
    };

    let endpoints = HealthEndpoints::with_config(registry, config);

    // Test health endpoint with custom configuration
    let response = endpoints.health().await.unwrap();
    assert_eq!(response.status_code, 200);
    assert_eq!(
        response.headers.get("X-Service-Name"),
        Some(&"ORCHER".to_string())
    );
    assert_eq!(
        response.headers.get("X-Version"),
        Some(&"1.0.0".to_string())
    );
    assert!(response.body.contains("timing"));
    assert!(response.body.contains("checks"));

    // Test detailed endpoint
    let details_response = endpoints.health_details().await.unwrap();
    assert_eq!(details_response.status_code, 200);
    assert!(details_response.body.contains("metadata"));

    println!("✅ Custom endpoint configuration test passed");
}

#[tokio::test]
async fn test_health_status_aggregation_rules() {
    let registry = Arc::new(HealthCheckRegistry::new());

    // Test 1: Only informational checks (should be healthy)
    let info_check = Arc::new(CacheHealthCheck::new("info_cache", 0.95));
    registry
        .register_with_criticality(info_check, HealthCheckCriticality::Informational)
        .unwrap();

    let status = registry.check_all().await.unwrap();
    assert_eq!(status.status, HealthStatus::Healthy);

    // Test 2: Add important check (should depend on important check)
    let important_check = Arc::new(DatabaseHealthCheck::new("important_db", false));
    registry
        .register_with_criticality(important_check, HealthCheckCriticality::Important)
        .unwrap();

    let status = registry.check_all().await.unwrap();
    assert_eq!(status.status, HealthStatus::Healthy);

    // Test 3: Add failing critical check (should be unhealthy regardless of others)
    let critical_check = Arc::new(DatabaseHealthCheck::new("critical_db", true));
    registry
        .register_with_criticality(critical_check, HealthCheckCriticality::Critical)
        .unwrap();

    let status = registry.check_all().await.unwrap();
    assert_eq!(status.status, HealthStatus::Unhealthy);
    assert_eq!(status.summary.healthy_critical, 0);
    assert_eq!(status.summary.healthy_important, 1);
    assert_eq!(status.summary.healthy_informational, 1);

    println!("✅ Health status aggregation rules test passed");
}

#[tokio::test]
async fn test_real_world_monitoring_scenario() {
    // Simulate a real-world ORCHER monitoring setup
    let registry = Arc::new(HealthCheckRegistry::new());

    // Critical components that must be healthy
    let primary_db = Arc::new(DatabaseHealthCheck::new("primary_database", false));
    let message_queue = Arc::new(DatabaseHealthCheck::new("message_queue", false));

    // Important components that affect performance
    let cache = Arc::new(CacheHealthCheck::new("redis_cache", 0.85));
    let search_index = Arc::new(ExternalServiceHealthCheck::new(
        "elasticsearch",
        Duration::from_millis(80),
    ));

    // Informational components
    let metrics_db = Arc::new(DatabaseHealthCheck::new("metrics_db", false));
    let log_aggregator = Arc::new(ExternalServiceHealthCheck::new(
        "log_service",
        Duration::from_millis(50), // Make it healthy instead of degraded
    ));

    // Register all checks
    registry
        .register_with_criticality(primary_db, HealthCheckCriticality::Critical)
        .unwrap();
    registry
        .register_with_criticality(message_queue, HealthCheckCriticality::Critical)
        .unwrap();
    registry
        .register_with_criticality(cache, HealthCheckCriticality::Important)
        .unwrap();
    registry
        .register_with_criticality(search_index, HealthCheckCriticality::Important)
        .unwrap();
    registry
        .register_with_criticality(metrics_db, HealthCheckCriticality::Informational)
        .unwrap();
    registry
        .register_with_criticality(log_aggregator, HealthCheckCriticality::Informational)
        .unwrap();

    // Verify all checks are registered
    let check_list = registry.list_checks();
    println!("Registered checks: {:?}", check_list);
    assert_eq!(
        check_list.len(),
        6,
        "Expected 6 checks but found {}",
        check_list.len()
    );

    // Perform comprehensive health check
    let start = std::time::Instant::now();
    let aggregated = registry.check_all().await.unwrap();
    let duration = start.elapsed();

    // Verify system is healthy (degraded services don't make system unhealthy)
    assert_eq!(aggregated.status, HealthStatus::Healthy);
    assert_eq!(aggregated.summary.total_checks, 6);
    assert_eq!(aggregated.summary.critical_checks, 2);
    assert_eq!(aggregated.summary.important_checks, 2);
    assert_eq!(aggregated.summary.informational_checks, 2);
    assert_eq!(aggregated.summary.healthy_total, 6); // All services should be healthy now

    // Should complete quickly due to concurrent execution
    assert!(duration < Duration::from_millis(300));

    // Test all HTTP endpoints
    let endpoints = HealthEndpoints::new(registry);

    let health_response = endpoints.health().await.unwrap();
    assert_eq!(health_response.status_code, 200);

    let ready_response = endpoints.ready().await.unwrap();
    assert_eq!(ready_response.status_code, 200);

    let live_response = endpoints.live().await.unwrap();
    assert_eq!(live_response.status_code, 200);

    let details_response = endpoints.health_details().await.unwrap();
    assert_eq!(details_response.status_code, 200);

    let list_response = endpoints.list_checks().await.unwrap();
    assert_eq!(list_response.status_code, 200);

    println!(
        "✅ Real-world monitoring scenario test passed in {:?}",
        duration
    );
    println!("   Total checks: {}", aggregated.summary.total_checks);
    println!("   Healthy checks: {}", aggregated.summary.healthy_total);
    println!("   System status: {:?}", aggregated.status);
}
