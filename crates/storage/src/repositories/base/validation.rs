//! # Data Validation
//!
//! Data validation utilities before persistence using orcher-common validation.

use orcher_common::{validation::Validatable, OrcherResult};

/// Validate an entity before persistence
pub fn validate_entity<T>(entity: &T) -> OrcherResult<()>
where
    T: Validatable,
{
    let result = entity.validate();
    if result.is_valid() {
        Ok(())
    } else {
        Err(crate::error::StorageError::ValidationFailed {
            field: "entity".to_string(),
            message: format!("Validation failed with {} errors", result.error_count()),
        }
        .into())
    }
}

#[cfg(test)]
mod tests {

    #[test]
    fn test_validation_module_exists() {
        // Compile-time test to ensure module compiles
        // Module existence is verified by successful compilation
    }
}
