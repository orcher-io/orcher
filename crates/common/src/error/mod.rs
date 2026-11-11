//! Unified error handling for the ORCHER platform.

use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum OrcherError {
    InvalidInput {
        message: String,
        field: Option<String>,
    },
    NotFound {
        resource_type: String,
        id: String,
    },
    Timeout {
        operation: String,
        timeout_secs: u64,
    },
    Internal {
        message: String,
        source: Option<String>,
    },
    Authentication {
        message: String,
    },
    Authorization {
        message: String,
        required_permission: Option<String>,
    },
    RateLimit {
        message: String,
        retry_after_secs: Option<u64>,
    },
    ExternalService {
        service: String,
        message: String,
        status_code: Option<u16>,
    },
    Configuration {
        key: String,
        message: String,
    },
    Serialization {
        /// e.g. "json", "messagepack"
        format: String,
        message: String,
    },
    Validation {
        field: String,
        message: String,
        rule: Option<String>,
    },
    ResourceExhausted {
        resource: String,
        message: String,
    },
    Workflow {
        workflow_id: String,
        message: String,
    },
    Scheduling {
        message: String,
    },
    Storage {
        operation: String,
        message: String,
    },
}

impl fmt::Display for OrcherError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            OrcherError::InvalidInput { message, field } => {
                if let Some(field) = field {
                    write!(f, "Invalid input for field '{}': {}", field, message)
                } else {
                    write!(f, "Invalid input: {}", message)
                }
            }
            OrcherError::NotFound { resource_type, id } => {
                write!(f, "{} with id '{}' not found", resource_type, id)
            }
            OrcherError::Timeout {
                operation,
                timeout_secs,
            } => {
                write!(
                    f,
                    "Operation '{}' timed out after {} seconds",
                    operation, timeout_secs
                )
            }
            OrcherError::Internal { message, .. } => {
                write!(f, "Internal error: {}", message)
            }
            OrcherError::Authentication { message } => {
                write!(f, "Authentication error: {}", message)
            }
            OrcherError::Authorization {
                message,
                required_permission,
            } => {
                if let Some(permission) = required_permission {
                    write!(
                        f,
                        "Authorization error: {} (required permission: {})",
                        message, permission
                    )
                } else {
                    write!(f, "Authorization error: {}", message)
                }
            }
            OrcherError::RateLimit {
                message,
                retry_after_secs,
            } => {
                if let Some(retry_after) = retry_after_secs {
                    write!(
                        f,
                        "Rate limit exceeded: {} (retry after {} seconds)",
                        message, retry_after
                    )
                } else {
                    write!(f, "Rate limit exceeded: {}", message)
                }
            }
            OrcherError::ExternalService {
                service,
                message,
                status_code,
            } => {
                if let Some(code) = status_code {
                    write!(
                        f,
                        "External service '{}' error ({}): {}",
                        service, code, message
                    )
                } else {
                    write!(f, "External service '{}' error: {}", service, message)
                }
            }
            OrcherError::Configuration { key, message } => {
                write!(f, "Configuration error for '{}': {}", key, message)
            }
            OrcherError::Serialization { format, message } => {
                write!(f, "Serialization error ({}): {}", format, message)
            }
            OrcherError::Validation {
                field,
                message,
                rule,
            } => {
                if let Some(rule) = rule {
                    write!(
                        f,
                        "Validation error for field '{}' (rule: {}): {}",
                        field, rule, message
                    )
                } else {
                    write!(f, "Validation error for field '{}': {}", field, message)
                }
            }
            OrcherError::ResourceExhausted { resource, message } => {
                write!(f, "Resource exhausted: {}: {}", resource, message)
            }
            OrcherError::Workflow {
                workflow_id,
                message,
            } => {
                write!(f, "Workflow error: {}: {}", workflow_id, message)
            }
            OrcherError::Scheduling { message } => {
                write!(f, "Scheduling error: {}", message)
            }
            OrcherError::Storage { operation, message } => {
                write!(f, "Storage error: {}: {}", operation, message)
            }
        }
    }
}

impl std::error::Error for OrcherError {}

pub type OrcherResult<T> = Result<T, OrcherError>;

impl OrcherError {
    pub fn invalid_input(message: impl Into<String>) -> Self {
        Self::InvalidInput {
            message: message.into(),
            field: None,
        }
    }

    pub fn invalid_input_field(field: impl Into<String>, message: impl Into<String>) -> Self {
        Self::InvalidInput {
            message: message.into(),
            field: Some(field.into()),
        }
    }

    pub fn not_found(resource_type: impl Into<String>, id: impl Into<String>) -> Self {
        Self::NotFound {
            resource_type: resource_type.into(),
            id: id.into(),
        }
    }

    pub fn timeout(operation: impl Into<String>, timeout_secs: u64) -> Self {
        Self::Timeout {
            operation: operation.into(),
            timeout_secs,
        }
    }

    pub fn internal(message: impl Into<String>) -> Self {
        Self::Internal {
            message: message.into(),
            source: None,
        }
    }

    pub fn authentication(message: impl Into<String>) -> Self {
        Self::Authentication {
            message: message.into(),
        }
    }

    pub fn authorization(message: impl Into<String>) -> Self {
        Self::Authorization {
            message: message.into(),
            required_permission: None,
        }
    }

    pub fn validation(field: impl Into<String>, message: impl Into<String>) -> Self {
        Self::Validation {
            field: field.into(),
            message: message.into(),
            rule: None,
        }
    }

    pub fn is_retryable(&self) -> bool {
        matches!(
            self,
            OrcherError::Timeout { .. }
                | OrcherError::RateLimit { .. }
                | OrcherError::ExternalService { .. }
                | OrcherError::Internal { .. }
                | OrcherError::ResourceExhausted { .. }
                | OrcherError::Scheduling { .. }
                | OrcherError::Storage { .. }
        )
    }

    pub fn http_status(&self) -> u16 {
        match self {
            OrcherError::InvalidInput { .. } => 400,
            OrcherError::NotFound { .. } => 404,
            OrcherError::Timeout { .. } => 408,
            OrcherError::Internal { .. } => 500,
            OrcherError::Authentication { .. } => 401,
            OrcherError::Authorization { .. } => 403,
            OrcherError::RateLimit { .. } => 429,
            OrcherError::ExternalService { status_code, .. } => status_code.unwrap_or(502),
            OrcherError::Configuration { .. } => 500,
            OrcherError::Serialization { .. } => 400,
            OrcherError::Validation { .. } => 400,
            OrcherError::ResourceExhausted { .. } => 503,
            OrcherError::Workflow { .. } => 500,
            OrcherError::Scheduling { .. } => 500,
            OrcherError::Storage { .. } => 500,
        }
    }

    /// Safe for display to end users (no internal details leaked).
    pub fn user_message(&self) -> String {
        match self {
            OrcherError::InvalidInput { message, .. } => {
                format!("Invalid input: {}", message)
            }
            OrcherError::NotFound { resource_type, .. } => {
                format!("{} not found", resource_type)
            }
            OrcherError::Timeout { operation, .. } => {
                format!("Operation '{}' took too long to complete", operation)
            }
            OrcherError::Internal { .. } => {
                "An internal error occurred. Please try again later.".to_string()
            }
            OrcherError::Authentication { .. } => {
                "Authentication required. Please log in.".to_string()
            }
            OrcherError::Authorization { .. } => {
                "You don't have permission to perform this action.".to_string()
            }
            OrcherError::RateLimit { .. } => {
                "Too many requests. Please try again later.".to_string()
            }
            OrcherError::ExternalService { service, .. } => {
                format!("External service '{}' is currently unavailable", service)
            }
            OrcherError::Configuration { .. } => {
                "Configuration error. Please check your settings.".to_string()
            }
            OrcherError::Serialization { .. } => {
                "Data format error. Please check your input.".to_string()
            }
            OrcherError::Validation { field, message, .. } => {
                format!("Validation error for {}: {}", field, message)
            }
            OrcherError::ResourceExhausted { resource, .. } => {
                format!(
                    "System resource '{}' is exhausted. Please try again later.",
                    resource
                )
            }
            OrcherError::Workflow { workflow_id, .. } => {
                format!("Workflow '{}' failed to execute", workflow_id)
            }
            OrcherError::Scheduling { .. } => {
                "Scheduling error occurred. Please try again.".to_string()
            }
            OrcherError::Storage { operation, .. } => {
                format!("Storage operation '{}' failed", operation)
            }
        }
    }
}

#[macro_export]
macro_rules! orcher_error {
    (invalid_input, $msg:expr) => {
        $crate::error::OrcherError::invalid_input($msg)
    };
    (invalid_input, $field:expr, $msg:expr) => {
        $crate::error::OrcherError::invalid_input_field($field, $msg)
    };
    (not_found, $type:expr, $id:expr) => {
        $crate::error::OrcherError::not_found($type, $id)
    };
    (timeout, $op:expr, $secs:expr) => {
        $crate::error::OrcherError::timeout($op, $secs)
    };
    (internal, $msg:expr) => {
        $crate::error::OrcherError::internal($msg)
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_creation() {
        let error = OrcherError::invalid_input("test message");
        assert!(matches!(error, OrcherError::InvalidInput { .. }));
    }

    #[test]
    fn test_error_display() {
        let error = OrcherError::not_found("Task", "123");
        let display = format!("{}", error);
        assert!(display.contains("Task"));
        assert!(display.contains("123"));
        assert!(display.contains("not found"));
    }

    #[test]
    fn test_http_status() {
        assert_eq!(OrcherError::invalid_input("test").http_status(), 400);
        assert_eq!(OrcherError::not_found("Task", "123").http_status(), 404);
        assert_eq!(OrcherError::internal("test").http_status(), 500);
        assert_eq!(OrcherError::authentication("test").http_status(), 401);
        assert_eq!(OrcherError::authorization("test").http_status(), 403);
    }

    #[test]
    fn test_retryable() {
        assert!(OrcherError::timeout("test", 30).is_retryable());
        assert!(OrcherError::internal("test").is_retryable());
        assert!(!OrcherError::invalid_input("test").is_retryable());
        assert!(!OrcherError::authentication("test").is_retryable());
    }

    #[test]
    fn test_user_message() {
        let error = OrcherError::internal("system error");
        let user_msg = error.user_message();
        assert!(!user_msg.contains("system error")); // Should not expose internal details
        assert!(user_msg.contains("internal error"));
    }

    #[test]
    fn test_orcher_result() {
        let success: OrcherResult<i32> = Ok(42);
        let failure: OrcherResult<i32> = Err(OrcherError::invalid_input("test"));

        assert!(success.is_ok());
        assert!(failure.is_err());
    }

    #[test]
    fn test_error_macros() {
        let error = orcher_error!(invalid_input, "test message");
        assert!(matches!(error, OrcherError::InvalidInput { .. }));

        let error = orcher_error!(not_found, "Task", "123");
        assert!(matches!(error, OrcherError::NotFound { .. }));
    }
}
