//! # Redis Test Container
//!
//! This module provides Redis test container management for integration testing,
//! using testcontainers to spin up temporary Redis instances.

#[cfg(test)]
use testcontainers::{core::WaitFor, ContainerRequest, GenericImage};

#[cfg(test)]
/// Redis test container configuration
pub struct RedisContainer {
    /// Redis port
    pub port: u16,
}

#[cfg(test)]
impl Default for RedisContainer {
    fn default() -> Self {
        Self { port: 6379 }
    }
}

#[cfg(test)]
impl RedisContainer {
    /// Create a new Redis test container configuration
    pub fn new() -> Self {
        Self::default()
    }

    /// Get the connection string for this container
    pub fn connection_string(&self, host: &str, port: u16) -> String {
        format!("redis://{}:{}", host, port)
    }

    /// Create a ContainerRequest for Redis
    pub fn image(&self) -> ContainerRequest<GenericImage> {
        GenericImage::new("redis", "7-alpine")
            .with_wait_for(WaitFor::message_on_stdout("Ready to accept connections"))
            .into()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_redis_container_default() {
        let container = RedisContainer::default();
        assert_eq!(container.port, 6379);
    }

    #[test]
    fn test_redis_container_new() {
        let container = RedisContainer::new();
        assert_eq!(container.port, 6379);
    }

    #[test]
    fn test_connection_string() {
        let container = RedisContainer::default();
        let conn_str = container.connection_string("localhost", 6379);
        assert_eq!(conn_str, "redis://localhost:6379");
        assert!(conn_str.starts_with("redis://"));
    }

    #[test]
    fn test_image_creation() {
        let container = RedisContainer::default();
        let _image = container.image();
        // ContainerRequest doesn't expose name() and tag() directly
        // Just verify it builds without error
    }
}
