//! Health checking module for ORCHER monitoring system
//!
//! This module provides health checks for monitoring components to ensure
//! the monitoring system itself is operating correctly.

pub mod check;
pub mod checks;
pub mod circuit_breaker;
pub mod endpoints;
pub mod monitoring_health;
pub mod registry;

// Re-export core health check types
pub use check::{AsyncHealthCheck, HealthCheckConfig, HealthCheckCriticality, HealthCheckResult};
pub use circuit_breaker::{HealthCheckCircuitBreaker, HealthCheckCircuitBreakerFactory};
pub use endpoints::{HealthEndpointConfig, HealthEndpoints, HealthResponse};
pub use registry::{
    AggregatedHealthStatus, HealthCheckRegistry, HealthRegistryConfig, HealthSummary,
};

// Re-export existing monitoring health types
pub use monitoring_health::{ExporterHealthCheck, MonitoringSystemHealthCheck};

// Re-export built-in health checks
pub use checks::{
    CustomHealthCheck, CustomHealthCheckBuilder, DatabaseHealthCheck, RedisHealthCheck,
    SchedulerHealthCheck, SecuritySystemHealthCheck, StorageHealthCheck,
    SystemResourcesHealthCheck, WorkerPoolHealthCheck,
};
