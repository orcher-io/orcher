//! `Validatable` trait and core validation types.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Types that support validation.
pub trait Validatable {
    fn validate(&self) -> ValidationResult;

    fn is_valid(&self) -> bool {
        self.validate().is_valid()
    }

    fn validation_errors(&self) -> Vec<ValidationError> {
        self.validate().errors
    }
}

/// Collected errors, warnings, and context from a validation pass.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationResult {
    pub errors: Vec<ValidationError>,
    pub warnings: Vec<ValidationWarning>,
    pub context: HashMap<String, String>,
}

impl ValidationResult {
    pub fn new() -> Self {
        Self {
            errors: Vec::new(),
            warnings: Vec::new(),
            context: HashMap::new(),
        }
    }

    pub fn is_valid(&self) -> bool {
        self.errors.is_empty()
    }

    pub fn has_warnings(&self) -> bool {
        !self.warnings.is_empty()
    }

    pub fn add_error(&mut self, field: impl Into<String>, message: impl Into<String>) {
        self.errors.push(ValidationError {
            field: field.into(),
            message: message.into(),
            code: None,
            severity: ValidationSeverity::Error,
        });
    }

    pub fn add_warning(&mut self, field: impl Into<String>, message: impl Into<String>) {
        self.warnings.push(ValidationWarning {
            field: field.into(),
            message: message.into(),
            code: None,
        });
    }

    pub fn add_context(&mut self, key: impl Into<String>, value: impl Into<String>) {
        self.context.insert(key.into(), value.into());
    }

    pub fn merge(&mut self, other: ValidationResult) {
        self.errors.extend(other.errors);
        self.warnings.extend(other.warnings);
        self.context.extend(other.context);
    }

    pub fn errors_for_field(&self, field: &str) -> Vec<&ValidationError> {
        self.errors.iter().filter(|e| e.field == field).collect()
    }

    pub fn error_count(&self) -> usize {
        self.errors.len()
    }

    pub fn warning_count(&self) -> usize {
        self.warnings.len()
    }
}

impl Default for ValidationResult {
    fn default() -> Self {
        Self::new()
    }
}

/// A single validation error.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationError {
    pub field: String,
    pub message: String,
    pub code: Option<String>,
    pub severity: ValidationSeverity,
}

impl ValidationError {
    pub fn new(field: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            field: field.into(),
            message: message.into(),
            code: None,
            severity: ValidationSeverity::Error,
        }
    }

    pub fn with_code(
        field: impl Into<String>,
        message: impl Into<String>,
        code: impl Into<String>,
    ) -> Self {
        Self {
            field: field.into(),
            message: message.into(),
            code: Some(code.into()),
            severity: ValidationSeverity::Error,
        }
    }

    pub fn critical(field: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            field: field.into(),
            message: message.into(),
            code: None,
            severity: ValidationSeverity::Critical,
        }
    }
}

/// A single validation warning.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationWarning {
    pub field: String,
    pub message: String,
    pub code: Option<String>,
}

impl ValidationWarning {
    pub fn new(field: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            field: field.into(),
            message: message.into(),
            code: None,
        }
    }
}

/// Severity level of a validation issue.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ValidationSeverity {
    /// Does not prevent the operation.
    Warning,
    /// Prevents the operation.
    Error,
    /// Indicates a serious system issue.
    Critical,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug)]
    struct TestStruct {
        name: String,
        email: String,
    }

    impl Validatable for TestStruct {
        fn validate(&self) -> ValidationResult {
            let mut result = ValidationResult::new();
            if self.name.is_empty() {
                result.add_error("name", "Name cannot be empty");
            }
            if !self.email.contains('@') {
                result.add_error("email", "Invalid email format");
            }
            result
        }
    }

    #[test]
    fn test_validation_result() {
        let mut result = ValidationResult::new();
        assert!(result.is_valid());
        assert!(!result.has_warnings());

        result.add_error("field1", "Error message");
        assert!(!result.is_valid());
        assert_eq!(result.error_count(), 1);

        result.add_warning("field2", "Warning message");
        assert!(result.has_warnings());
        assert_eq!(result.warning_count(), 1);
    }

    #[test]
    fn test_validatable_trait() {
        let valid_struct = TestStruct {
            name: "John Doe".to_string(),
            email: "john@example.com".to_string(),
        };
        assert!(valid_struct.is_valid());

        let invalid_struct = TestStruct {
            name: "".to_string(),
            email: "invalid-email".to_string(),
        };
        assert!(!invalid_struct.is_valid());
        assert_eq!(invalid_struct.validation_errors().len(), 2);
    }

    #[test]
    fn test_validation_error_creation() {
        let error = ValidationError::new("test_field", "Test message");
        assert_eq!(error.field, "test_field");
        assert_eq!(error.message, "Test message");
        assert_eq!(error.severity, ValidationSeverity::Error);

        let critical_error = ValidationError::critical("critical_field", "Critical issue");
        assert_eq!(critical_error.severity, ValidationSeverity::Critical);
    }

    #[test]
    fn test_validation_result_merge() {
        let mut result1 = ValidationResult::new();
        result1.add_error("field1", "Error 1");
        result1.add_warning("field1", "Warning 1");

        let mut result2 = ValidationResult::new();
        result2.add_error("field2", "Error 2");
        result2.add_warning("field2", "Warning 2");

        result1.merge(result2);

        assert_eq!(result1.errors.len(), 2);
        assert_eq!(result1.warnings.len(), 2);
        assert!(!result1.is_valid());
    }
}
