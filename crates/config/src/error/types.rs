//! Configuration error types.

use orcher_common::error::OrcherError;

/// Configuration errors.
#[derive(Debug, Clone, thiserror::Error)]
pub enum ConfigError {
    /// Configuration file not found
    #[error("Configuration file not found: {path}")]
    FileNotFound {
        /// Path to the missing configuration file
        path: String,
    },

    /// Invalid configuration format
    #[error("Invalid configuration format in {file_path}: {message}")]
    InvalidFormat {
        /// Path to the configuration file with invalid format
        file_path: String,
        /// Error message describing the format issue
        message: String,
    },

    /// Configuration validation failed
    #[error("Configuration validation failed: {errors:?}")]
    ValidationFailed {
        /// List of validation error messages
        errors: Vec<String>,
    },

    /// Invalid configuration value
    #[error("Invalid configuration value for {field}: {value} - {reason}")]
    InvalidValue {
        /// Configuration field name
        field: String,
        /// Invalid value
        value: String,
        /// Reason why value is invalid
        reason: String,
    },

    /// Missing required configuration field
    #[error("Missing required configuration field: {field}")]
    MissingField {
        /// Required field name
        field: String,
    },

    /// IO error during configuration operations
    #[error("IO error during configuration operation: {operation} - {message}")]
    IoError {
        /// Operation that failed
        operation: String,
        /// Error message
        message: String,
    },

    /// Internal configuration system error
    #[error("Internal configuration system error: {message}")]
    InternalError {
        /// Error message
        message: String,
    },
}

impl From<ConfigError> for OrcherError {
    fn from(err: ConfigError) -> Self {
        match err {
            ConfigError::FileNotFound { .. } => {
                OrcherError::not_found("config_file".to_string(), err.to_string())
            }
            ConfigError::ValidationFailed { .. }
            | ConfigError::InvalidValue { .. }
            | ConfigError::MissingField { .. }
            | ConfigError::InvalidFormat { .. } => OrcherError::invalid_input(err.to_string()),
            _ => OrcherError::internal(err.to_string()),
        }
    }
}

impl From<std::io::Error> for ConfigError {
    fn from(err: std::io::Error) -> Self {
        ConfigError::IoError {
            operation: "io_operation".to_string(),
            message: err.to_string(),
        }
    }
}

/// Result type for configuration operations
pub type ConfigResult<T> = Result<T, ConfigError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_display() {
        let error = ConfigError::FileNotFound {
            path: "config.yaml".to_string(),
        };
        assert!(error.to_string().contains("config.yaml"));

        let error = ConfigError::ValidationFailed {
            errors: vec!["Invalid port".to_string()],
        };
        assert!(error.to_string().contains("validation"));
    }

    #[test]
    fn test_error_conversion_to_orcher_error() {
        let config_error = ConfigError::FileNotFound {
            path: "test.yaml".to_string(),
        };
        let orcher_error: OrcherError = config_error.into();
        assert!(orcher_error.to_string().contains("test.yaml"));
    }
}
