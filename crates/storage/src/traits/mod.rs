//! # Storage Traits Module
//!
//! This module contains all storage trait definitions using orcher-common types
//! for maximum compatibility and type safety across the ORCHER ecosystem.

/// Core storage trait definitions
pub mod core;

/// Domain-specific storage trait definitions
pub mod domain;

// Re-export commonly used traits for convenience
pub use core::{BatchRepository, Repository};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_traits_module_exports() {
        // Compile-time test to ensure all traits are properly exported
        fn _assert_traits_available<T, R>()
        where
            T: Clone + Send + Sync + orcher_common::validation::Validatable + 'static,
            R: Repository<T> + BatchRepository<T>,
        {
        }
    }
}
