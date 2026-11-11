//! # PostgreSQL Repository Module
//!
//! Repository implementations for PostgreSQL backend with orcher-common integration.

/// Base repository with common CRUD operations
pub mod base;

// Re-export main types for convenience
pub use base::BaseRepository;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_repository_module_exports() {
        // Compile-time test to ensure all repository types are properly exported
        fn _check_base<U>(_: BaseRepository<U>) {}
        // Module exports verified by successful compilation
    }
}
