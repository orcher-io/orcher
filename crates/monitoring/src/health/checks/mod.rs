//! Health checks module for ORCHER components
//!
//! This module provides built-in health checks for all ORCHER platform components,
//! including database, Redis, storage, worker pools, scheduler, security systems
//! and system resources.

/// Database connectivity health checks
pub mod database;

/// Redis connectivity and command execution health checks
pub mod redis;

/// Storage system health checks including disk space and connection pools
pub mod storage;

/// Worker pool health checks monitoring active workers and queue status
pub mod worker;

/// Scheduler health checks validating trigger processing and job queue status
pub mod scheduler;

/// Security system health checks for auth services and rate limiting
pub mod security;

/// System resources health checks for CPU, memory, and disk usage thresholds
pub mod system;

/// Custom health check helpers for user-defined health checks
pub mod custom;

// Re-export health check implementations
pub use custom::{CustomHealthCheck, CustomHealthCheckBuilder};
pub use database::DatabaseHealthCheck;
pub use redis::RedisHealthCheck;
pub use scheduler::SchedulerHealthCheck;
pub use security::SecuritySystemHealthCheck;
pub use storage::StorageHealthCheck;
pub use system::SystemResourcesHealthCheck;
pub use worker::WorkerPoolHealthCheck;
