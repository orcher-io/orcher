//! # Test Container Management
//!
//! This module provides test container management for integration testing,
//! using testcontainers to spin up PostgreSQL and Redis instances for tests.

#[cfg(test)]
pub mod postgres;

#[cfg(test)]
pub mod redis;

// Re-export for convenience
#[cfg(test)]
pub use postgres::*;

#[cfg(test)]
pub use redis::*;
