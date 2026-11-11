//! # Core Storage Traits
//!
//! This module contains the fundamental storage trait definitions using orcher-common
//! types for maximum compatibility and type safety across the ORCHER ecosystem.

/// Core repository trait definitions
pub mod repository;

/// Entity persistence trait definitions
pub mod persistable;

/// Cache strategy trait definitions
pub mod cacheable;

/// Advanced querying trait definitions
pub mod queryable;

// Re-export commonly used traits for convenience
pub use cacheable::{AdvancedCacheable, Cacheable, CacheableCollection};
pub use persistable::{
    Auditable, Persistable, SerializablePersistable, SoftDeletable, Timestamped,
};
pub use queryable::{AdvancedQueryable, Query, QueryResult, Queryable};
pub use repository::{BatchRepository, Repository};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_core_traits_module_exports() {
        // Compile-time test to ensure all traits are properly exported
        fn _assert_traits_available<T, R>()
        where
            T: Clone
                + Send
                + Sync
                + orcher_common::validation::Validatable
                + 'static
                + serde::Serialize
                + for<'de> serde::Deserialize<'de>
                + std::fmt::Debug,
            R: Repository<T> + BatchRepository<T>,
        {
        }

        fn _assert_persistable_trait<T>()
        where
            T: Persistable + Cacheable + Queryable,
        {
        }
    }
}
