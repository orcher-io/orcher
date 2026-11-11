//! # PostgreSQL Test Container
//!
//! This module provides PostgreSQL test container management for integration testing,
//! using testcontainers to spin up temporary PostgreSQL instances.

#[cfg(test)]
use testcontainers::{core::WaitFor, ContainerRequest, GenericImage, ImageExt};

#[cfg(test)]
/// PostgreSQL test container configuration
pub struct PostgresContainer {
    /// Database name
    pub database: String,
    /// Database user
    pub user: String,
    /// Database password
    pub password: String,
}

#[cfg(test)]
impl Default for PostgresContainer {
    fn default() -> Self {
        Self {
            database: "test_db".to_string(),
            user: "test_user".to_string(),
            password: "test_password".to_string(),
        }
    }
}

#[cfg(test)]
impl PostgresContainer {
    /// Create a new PostgreSQL test container configuration
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the database name
    pub fn with_database(mut self, database: impl Into<String>) -> Self {
        self.database = database.into();
        self
    }

    /// Set the database user
    pub fn with_user(mut self, user: impl Into<String>) -> Self {
        self.user = user.into();
        self
    }

    /// Set the database password
    pub fn with_password(mut self, password: impl Into<String>) -> Self {
        self.password = password.into();
        self
    }

    /// Get the connection string for this container
    pub fn connection_string(&self, host: &str, port: u16) -> String {
        format!(
            "postgresql://{}:{}@{}:{}/{}",
            self.user, self.password, host, port, self.database
        )
    }

    /// Create a ContainerRequest for PostgreSQL
    pub fn image(&self) -> ContainerRequest<GenericImage> {
        GenericImage::new("postgres", "16-alpine")
            .with_wait_for(WaitFor::message_on_stderr(
                "database system is ready to accept connections",
            ))
            .with_env_var("POSTGRES_DB", self.database.clone())
            .with_env_var("POSTGRES_USER", self.user.clone())
            .with_env_var("POSTGRES_PASSWORD", self.password.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_postgres_container_default() {
        let container = PostgresContainer::default();
        assert_eq!(container.database, "test_db");
        assert_eq!(container.user, "test_user");
        assert_eq!(container.password, "test_password");
    }

    #[test]
    fn test_postgres_container_builder() {
        let container = PostgresContainer::new()
            .with_database("custom_db")
            .with_user("custom_user")
            .with_password("custom_pass");

        assert_eq!(container.database, "custom_db");
        assert_eq!(container.user, "custom_user");
        assert_eq!(container.password, "custom_pass");
    }

    #[test]
    fn test_connection_string() {
        let container = PostgresContainer::default();
        let conn_str = container.connection_string("localhost", 5432);
        assert!(conn_str.contains("postgresql://"));
        assert!(conn_str.contains("test_user"));
        assert!(conn_str.contains("test_password"));
        assert!(conn_str.contains("test_db"));
    }

    #[test]
    fn test_image_creation() {
        let container = PostgresContainer::default();
        let _image = container.image();
        // ContainerRequest doesn't expose name() and tag() directly
        // Just verify it builds without error
    }
}
