//! Health check registry for managing multiple health checks
//!
//! This module provides a centralized registry for managing health checks
//! with circuit breaker protection, timeout handling, and status aggregation.

use crate::error::Result;
use crate::health::check::{AsyncHealthCheck, HealthCheckCriticality, HealthCheckResult};
use crate::health::circuit_breaker::HealthCheckCircuitBreakerFactory;
use futures::future::join_all;
use orcher_common::monitoring::HealthStatus;
use orcher_common::utils::circuit_breaker::CircuitBreakerConfig;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Configuration for the health check registry
#[derive(Debug, Clone)]
pub struct HealthRegistryConfig {
    /// Global timeout for all health checks
    pub global_timeout: Duration,
    /// Maximum number of concurrent health checks
    pub max_concurrent_checks: usize,
    /// How long to cache health check results
    pub result_cache_duration: Duration,
    /// Whether to enable circuit breaker protection by default
    pub default_circuit_breaker_enabled: bool,
    /// Default circuit breaker configuration
    pub default_circuit_breaker_config: CircuitBreakerConfig,
}

impl Default for HealthRegistryConfig {
    fn default() -> Self {
        Self {
            global_timeout: Duration::from_secs(30),
            max_concurrent_checks: 20,
            result_cache_duration: Duration::from_secs(60),
            default_circuit_breaker_enabled: true,
            default_circuit_breaker_config: CircuitBreakerConfig {
                failure_threshold: 3,
                recovery_timeout: Duration::from_secs(60),
                success_threshold: 2,
                failure_window: Duration::from_secs(300),
                half_open_max_requests: 1,
            },
        }
    }
}

/// Cached health check result with expiration
#[derive(Debug, Clone)]
struct CachedHealthResult {
    result: HealthCheckResult,
    expires_at: Instant,
}

impl CachedHealthResult {
    fn new(result: HealthCheckResult, cache_duration: Duration) -> Self {
        Self {
            result,
            expires_at: Instant::now() + cache_duration,
        }
    }

    fn is_expired(&self) -> bool {
        Instant::now() > self.expires_at
    }
}

/// Registry entry for a health check
struct HealthCheckEntry {
    /// The health check wrapped with circuit breaker
    check: Arc<dyn AsyncHealthCheck>,
    /// Criticality level of this health check
    criticality: HealthCheckCriticality,
    /// Whether this check is enabled
    enabled: bool,
    /// Last execution result (cached)
    cached_result: Option<CachedHealthResult>,
}

/// Aggregated health status for the registry
#[derive(Debug, Clone)]
pub struct AggregatedHealthStatus {
    /// Overall health status
    pub status: HealthStatus,
    /// Individual health check results
    pub checks: HashMap<String, HealthCheckResult>,
    /// Summary of check results by criticality
    pub summary: HealthSummary,
    /// Total execution time for all checks
    pub total_duration: Duration,
    /// Timestamp when the aggregation was performed
    pub timestamp: Instant,
}

/// Summary of health check results
#[derive(Debug, Clone, Default)]
pub struct HealthSummary {
    /// Number of critical checks
    pub critical_checks: usize,
    /// Number of healthy critical checks
    pub healthy_critical: usize,
    /// Number of important checks
    pub important_checks: usize,
    /// Number of healthy important checks
    pub healthy_important: usize,
    /// Number of informational checks
    pub informational_checks: usize,
    /// Number of healthy informational checks
    pub healthy_informational: usize,
    /// Total number of checks
    pub total_checks: usize,
    /// Number of healthy checks
    pub healthy_total: usize,
}

/// Health check registry for managing multiple health checks
pub struct HealthCheckRegistry {
    /// Registry configuration
    config: HealthRegistryConfig,
    /// Registered health checks
    checks: RwLock<HashMap<String, HealthCheckEntry>>,
    /// Circuit breaker factory
    circuit_breaker_factory: HealthCheckCircuitBreakerFactory,
}

impl HealthCheckRegistry {
    /// Create a new health check registry
    pub fn new() -> Self {
        Self::with_config(HealthRegistryConfig::default())
    }

    /// Create a new health check registry with custom configuration
    pub fn with_config(config: HealthRegistryConfig) -> Self {
        let circuit_breaker_factory = HealthCheckCircuitBreakerFactory::with_default_config(
            config.default_circuit_breaker_config.clone(),
        );

        Self {
            config,
            checks: RwLock::new(HashMap::new()),
            circuit_breaker_factory,
        }
    }

    /// Register a health check with default settings
    pub fn register<T>(&self, check: Arc<T>) -> Result<()>
    where
        T: AsyncHealthCheck + 'static,
    {
        self.register_with_criticality(check, HealthCheckCriticality::Important)
    }

    /// Register a health check with specific criticality
    pub fn register_with_criticality<T>(
        &self,
        check: Arc<T>,
        criticality: HealthCheckCriticality,
    ) -> Result<()>
    where
        T: AsyncHealthCheck + 'static,
    {
        let name = check.name().to_string();

        // Wrap with circuit breaker if enabled
        let wrapped_check: Arc<dyn AsyncHealthCheck> =
            if self.config.default_circuit_breaker_enabled {
                Arc::new(self.circuit_breaker_factory.wrap(check))
            } else {
                check
            };

        let entry = HealthCheckEntry {
            check: wrapped_check,
            criticality,
            enabled: true,
            cached_result: None,
        };

        self.checks.write().insert(name, entry);
        Ok(())
    }

    /// Register a health check with custom circuit breaker configuration
    pub fn register_with_circuit_breaker<T>(
        &self,
        check: Arc<T>,
        criticality: HealthCheckCriticality,
        circuit_breaker_config: CircuitBreakerConfig,
    ) -> Result<()>
    where
        T: AsyncHealthCheck + 'static,
    {
        let name = check.name().to_string();

        let wrapped_check = Arc::new(
            self.circuit_breaker_factory
                .wrap_with_config(check, circuit_breaker_config),
        );

        let entry = HealthCheckEntry {
            check: wrapped_check,
            criticality,
            enabled: true,
            cached_result: None,
        };

        self.checks.write().insert(name, entry);
        Ok(())
    }

    /// Unregister a health check
    pub fn unregister(&self, name: &str) -> bool {
        self.checks.write().remove(name).is_some()
    }

    /// Enable or disable a health check
    pub fn set_enabled(&self, name: &str, enabled: bool) -> bool {
        if let Some(entry) = self.checks.write().get_mut(name) {
            entry.enabled = enabled;
            true
        } else {
            false
        }
    }

    /// Check if a health check is registered
    pub fn is_registered(&self, name: &str) -> bool {
        self.checks.read().contains_key(name)
    }

    /// Get the names of all registered health checks
    pub fn list_checks(&self) -> Vec<String> {
        self.checks.read().keys().cloned().collect()
    }

    /// Execute a single health check
    pub async fn check_one(&self, name: &str) -> Result<Option<HealthCheckResult>> {
        let entry = {
            let checks = self.checks.read();
            if let Some(entry) = checks.get(name) {
                if !entry.enabled {
                    return Ok(None);
                }
                entry.check.clone()
            } else {
                return Ok(None);
            }
        };

        let result = tokio::time::timeout(self.config.global_timeout, entry.check()).await;

        match result {
            Ok(Ok(health_result)) => {
                // Cache the result
                self.cache_result(name, health_result.clone());
                Ok(Some(health_result))
            }
            Ok(Err(e)) => Err(e),
            Err(_) => {
                // Timeout occurred
                let timeout_result = HealthCheckResult::new(name, HealthStatus::Unhealthy)
                    .with_message(format!(
                        "Health check timed out after {:?}",
                        self.config.global_timeout
                    ))
                    .with_duration(self.config.global_timeout);

                self.cache_result(name, timeout_result.clone());
                Ok(Some(timeout_result))
            }
        }
    }

    /// Execute all health checks and return aggregated status
    pub async fn check_all(&self) -> Result<AggregatedHealthStatus> {
        let start_time = Instant::now();
        let check_names: Vec<String>;

        // Collect enabled checks
        {
            let checks = self.checks.read();
            check_names = checks
                .iter()
                .filter(|(_, entry)| entry.enabled)
                .map(|(name, _)| name.clone())
                .collect();
        }

        // Execute all checks concurrently with limit
        let chunk_size = self.config.max_concurrent_checks;
        let mut results = HashMap::new();

        for chunk in check_names.chunks(chunk_size) {
            let chunk_futures = chunk.iter().map(|name| {
                let name = name.clone();
                async move {
                    let result = self.check_one(&name).await;
                    (name, result)
                }
            });

            let chunk_results = join_all(chunk_futures).await;

            for (name, result) in chunk_results {
                match result {
                    Ok(Some(health_result)) => {
                        results.insert(name, health_result);
                    }
                    Ok(None) => {
                        // Check was disabled, skip
                    }
                    Err(e) => {
                        // Create error result
                        let error_result = HealthCheckResult::new(&name, HealthStatus::Unhealthy)
                            .with_message(format!("Error executing health check: {}", e));
                        results.insert(name, error_result);
                    }
                }
            }
        }

        let total_duration = start_time.elapsed();
        let summary = self.calculate_summary(&results);
        let overall_status = self.determine_overall_status(&results, &summary);

        Ok(AggregatedHealthStatus {
            status: overall_status,
            checks: results,
            summary,
            total_duration,
            timestamp: start_time,
        })
    }

    /// Get cached results for all checks (doesn't execute checks)
    pub fn get_cached_results(&self) -> HashMap<String, HealthCheckResult> {
        let checks = self.checks.read();
        let mut results = HashMap::new();

        for (name, entry) in checks.iter() {
            if let Some(cached) = &entry.cached_result {
                if !cached.is_expired() {
                    results.insert(name.clone(), cached.result.clone());
                }
            }
        }

        results
    }

    /// Clear all cached results
    pub fn clear_cache(&self) {
        let mut checks = self.checks.write();
        for entry in checks.values_mut() {
            entry.cached_result = None;
        }
    }

    /// Cache a health check result
    fn cache_result(&self, name: &str, result: HealthCheckResult) {
        if let Some(entry) = self.checks.write().get_mut(name) {
            entry.cached_result = Some(CachedHealthResult::new(
                result,
                self.config.result_cache_duration,
            ));
        }
    }

    /// Calculate summary statistics
    fn calculate_summary(&self, results: &HashMap<String, HealthCheckResult>) -> HealthSummary {
        let checks = self.checks.read();
        let mut summary = HealthSummary {
            critical_checks: 0,
            healthy_critical: 0,
            important_checks: 0,
            healthy_important: 0,
            informational_checks: 0,
            healthy_informational: 0,
            total_checks: 0,
            healthy_total: 0,
        };

        for (name, result) in results {
            if let Some(entry) = checks.get(name) {
                summary.total_checks += 1;
                if result.is_healthy() {
                    summary.healthy_total += 1;
                }

                match entry.criticality {
                    HealthCheckCriticality::Critical => {
                        summary.critical_checks += 1;
                        if result.is_healthy() {
                            summary.healthy_critical += 1;
                        }
                    }
                    HealthCheckCriticality::Important => {
                        summary.important_checks += 1;
                        if result.is_healthy() {
                            summary.healthy_important += 1;
                        }
                    }
                    HealthCheckCriticality::Informational => {
                        summary.informational_checks += 1;
                        if result.is_healthy() {
                            summary.healthy_informational += 1;
                        }
                    }
                }
            }
        }

        summary
    }

    /// Determine overall health status based on criticality rules
    fn determine_overall_status(
        &self,
        results: &HashMap<String, HealthCheckResult>,
        summary: &HealthSummary,
    ) -> HealthStatus {
        let checks = self.checks.read();

        // System is unhealthy if any critical check fails
        for (name, result) in results {
            if let Some(entry) = checks.get(name) {
                if matches!(entry.criticality, HealthCheckCriticality::Critical)
                    && result.is_unhealthy()
                {
                    return HealthStatus::Unhealthy;
                }
            }
        }

        // System is healthy if all critical checks pass
        // Important and informational checks don't affect overall status
        if summary.critical_checks > 0 && summary.healthy_critical == summary.critical_checks {
            HealthStatus::Healthy
        } else if summary.critical_checks == 0 {
            // No critical checks, base decision on important checks
            if summary.important_checks > 0 {
                if summary.healthy_important == summary.important_checks {
                    HealthStatus::Healthy
                } else {
                    HealthStatus::Unhealthy
                }
            } else {
                // No critical or important checks, system is healthy by default
                HealthStatus::Healthy
            }
        } else {
            HealthStatus::Unhealthy
        }
    }
}

impl Default for HealthCheckRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::health::check::AsyncHealthCheck;

    /// Mock health check for testing
    struct MockHealthCheck {
        name: String,
        status: HealthStatus,
        delay: Duration,
    }

    impl MockHealthCheck {
        fn new(name: &str, status: HealthStatus) -> Self {
            Self {
                name: name.to_string(),
                status,
                delay: Duration::ZERO,
            }
        }

        fn with_delay(mut self, delay: Duration) -> Self {
            self.delay = delay;
            self
        }
    }

    #[async_trait::async_trait]
    impl AsyncHealthCheck for MockHealthCheck {
        fn name(&self) -> &str {
            &self.name
        }

        async fn check(&self) -> Result<HealthCheckResult> {
            if self.delay > Duration::ZERO {
                tokio::time::sleep(self.delay).await;
            }

            Ok(HealthCheckResult::new(&self.name, self.status.clone())
                .with_message(format!("Mock check result: {:?}", self.status)))
        }
    }

    #[tokio::test]
    async fn test_registry_creation() {
        let registry = HealthCheckRegistry::new();
        assert!(registry.list_checks().is_empty());
    }

    #[tokio::test]
    async fn test_register_and_check_single() {
        let registry = HealthCheckRegistry::new();
        let check = Arc::new(MockHealthCheck::new("test_check", HealthStatus::Healthy));

        registry.register(check).unwrap();
        assert!(registry.is_registered("test_check"));

        let result = registry.check_one("test_check").await.unwrap().unwrap();
        assert_eq!(result.component_name, "test_check");
        assert!(result.is_healthy());
    }

    #[tokio::test]
    async fn test_register_with_criticality() {
        let registry = HealthCheckRegistry::new();
        let check = Arc::new(MockHealthCheck::new(
            "critical_check",
            HealthStatus::Healthy,
        ));

        registry
            .register_with_criticality(check, HealthCheckCriticality::Critical)
            .unwrap();

        let status = registry.check_all().await.unwrap();
        assert_eq!(status.summary.critical_checks, 1);
        assert_eq!(status.summary.healthy_critical, 1);
    }

    #[tokio::test]
    async fn test_multiple_checks_aggregation() {
        let registry = HealthCheckRegistry::new();

        // Register multiple checks with different criticalities
        let critical_check = Arc::new(MockHealthCheck::new("critical", HealthStatus::Healthy));
        let important_check = Arc::new(MockHealthCheck::new("important", HealthStatus::Unhealthy));
        let info_check = Arc::new(MockHealthCheck::new("info", HealthStatus::Healthy));

        registry
            .register_with_criticality(critical_check, HealthCheckCriticality::Critical)
            .unwrap();
        registry
            .register_with_criticality(important_check, HealthCheckCriticality::Important)
            .unwrap();
        registry
            .register_with_criticality(info_check, HealthCheckCriticality::Informational)
            .unwrap();

        let status = registry.check_all().await.unwrap();

        // Overall should be healthy because critical check passes
        assert_eq!(status.status, HealthStatus::Healthy);
        assert_eq!(status.summary.total_checks, 3);
        assert_eq!(status.summary.healthy_total, 2);
        assert_eq!(status.summary.critical_checks, 1);
        assert_eq!(status.summary.healthy_critical, 1);
    }

    #[tokio::test]
    async fn test_critical_check_failure_makes_system_unhealthy() {
        let registry = HealthCheckRegistry::new();

        let critical_check = Arc::new(MockHealthCheck::new("critical", HealthStatus::Unhealthy));
        let important_check = Arc::new(MockHealthCheck::new("important", HealthStatus::Healthy));

        registry
            .register_with_criticality(critical_check, HealthCheckCriticality::Critical)
            .unwrap();
        registry
            .register_with_criticality(important_check, HealthCheckCriticality::Important)
            .unwrap();

        let status = registry.check_all().await.unwrap();

        // Overall should be unhealthy because critical check fails
        assert_eq!(status.status, HealthStatus::Unhealthy);
    }

    #[tokio::test]
    async fn test_check_timeout() {
        let config = HealthRegistryConfig {
            global_timeout: Duration::from_millis(100),
            ..Default::default()
        };
        let registry = HealthCheckRegistry::with_config(config);

        let slow_check = Arc::new(
            MockHealthCheck::new("slow", HealthStatus::Healthy)
                .with_delay(Duration::from_millis(200)),
        );

        registry.register(slow_check).unwrap();

        let result = registry.check_one("slow").await.unwrap().unwrap();
        assert!(result.is_unhealthy());
        assert!(result.message.unwrap().contains("timed out"));
    }

    #[tokio::test]
    async fn test_enable_disable_checks() {
        let registry = HealthCheckRegistry::new();
        let check = Arc::new(MockHealthCheck::new("test", HealthStatus::Healthy));

        registry.register(check).unwrap();
        assert!(registry.set_enabled("test", false));

        let result = registry.check_one("test").await.unwrap();
        assert!(result.is_none());

        assert!(registry.set_enabled("test", true));
        let result = registry.check_one("test").await.unwrap();
        assert!(result.is_some());
    }

    #[tokio::test]
    async fn test_unregister() {
        let registry = HealthCheckRegistry::new();
        let check = Arc::new(MockHealthCheck::new("test", HealthStatus::Healthy));

        registry.register(check).unwrap();
        assert!(registry.is_registered("test"));

        assert!(registry.unregister("test"));
        assert!(!registry.is_registered("test"));
        assert!(!registry.unregister("test")); // Already removed
    }

    #[tokio::test]
    async fn test_result_caching() {
        let registry = HealthCheckRegistry::new();
        let check = Arc::new(MockHealthCheck::new("cached", HealthStatus::Healthy));

        registry.register(check).unwrap();

        // First check should execute and cache result
        let result1 = registry.check_one("cached").await.unwrap().unwrap();
        assert!(result1.is_healthy());

        // Should have cached result
        let cached = registry.get_cached_results();
        assert!(cached.contains_key("cached"));

        // Clear cache
        registry.clear_cache();
        let cached = registry.get_cached_results();
        assert!(cached.is_empty());
    }

    #[tokio::test]
    async fn test_list_checks() {
        let registry = HealthCheckRegistry::new();

        let check1 = Arc::new(MockHealthCheck::new("check1", HealthStatus::Healthy));
        let check2 = Arc::new(MockHealthCheck::new("check2", HealthStatus::Healthy));

        registry.register(check1).unwrap();
        registry.register(check2).unwrap();

        let mut checks = registry.list_checks();
        checks.sort();
        assert_eq!(checks, vec!["check1", "check2"]);
    }
}
