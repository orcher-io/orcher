//! # Persistable Trait
//!
//! Core trait for entity persistence contracts using orcher-common types
//! for maximum compatibility and type safety across the ORCHER ecosystem.

use async_trait::async_trait;
use orcher_common::types::time::Timestamptz;
use orcher_common::{
    types::identifiers::traits::Identifier, validation::Validatable, OrcherResult,
};
use serde::{Deserialize, Serialize};
use std::fmt::Debug;

/// Core trait for entities that can be persisted to storage
///
/// This trait defines the fundamental contract for entities that can be stored,
/// retrieved, and managed in the ORCHER storage system. It leverages orcher-common
/// types to ensure consistency across all storage operations.
pub trait Persistable:
    Clone + Send + Sync + Validatable + Serialize + for<'de> Deserialize<'de> + Debug + 'static
{
    /// The identifier type for this entity
    type Id: Identifier + Send + Sync + Debug + Clone + 'static;

    /// Get the unique identifier for this entity
    fn id(&self) -> &Self::Id;

    /// Get the object type name for this entity (used for storage routing)
    fn object_type() -> &'static str;

    /// Get the storage table/collection name for this entity
    fn table_name() -> &'static str {
        Self::object_type()
    }

    /// Get the primary key field name (defaults to "id")
    fn primary_key() -> &'static str {
        "id"
    }

    /// Check if this entity is new (not yet persisted)
    fn is_new(&self) -> bool {
        // Default implementation - can be overridden
        false
    }

    /// Mark entity as persisted (called after successful save)
    fn mark_persisted(&mut self) {
        // Default implementation - can be overridden
    }

    /// Get the entity version for optimistic locking (if supported)
    fn version(&self) -> Option<i64> {
        None
    }

    /// Set the entity version (for optimistic locking)
    fn set_version(&mut self, _version: i64) {
        // Default implementation - can be overridden
    }

    /// Prepare entity for persistence (called before save)
    fn before_persist(&mut self) -> OrcherResult<()> {
        // Validate entity before persistence
        let validation_result = self.validate();
        if !validation_result.is_valid() {
            return Err(crate::error::StorageError::ValidationFailed {
                field: "entity".to_string(),
                message: format!(
                    "Entity validation failed with {} errors",
                    validation_result.error_count()
                ),
            }
            .into());
        }
        Ok(())
    }

    /// Post-processing after successful persistence
    fn after_persist(&mut self) -> OrcherResult<()> {
        self.mark_persisted();
        Ok(())
    }

    /// Prepare entity for deletion (called before delete)
    fn before_delete(&mut self) -> OrcherResult<()> {
        Ok(())
    }

    /// Post-processing after successful deletion
    fn after_delete(&mut self) -> OrcherResult<()> {
        Ok(())
    }
}

/// Trait for entities that support soft deletion
pub trait SoftDeletable: Persistable {
    /// Check if entity is soft deleted
    fn is_deleted(&self) -> bool;

    /// Mark entity as soft deleted
    fn mark_deleted(&mut self);

    /// Restore soft deleted entity
    fn restore(&mut self);

    /// Get deletion timestamp
    fn deleted_at(&self) -> Option<Timestamptz>;
}

/// Trait for entities that track creation and modification timestamps
pub trait Timestamped: Persistable {
    /// Get creation timestamp
    fn created_at(&self) -> Timestamptz;

    /// Get last modification timestamp
    fn updated_at(&self) -> Timestamptz;

    /// Update the modification timestamp
    fn touch(&mut self);
}

/// Trait for entities that support auditing
pub trait Auditable: Persistable + Timestamped {
    /// Type for representing user/system that created the entity
    type Actor: Send + Sync + Debug + Clone;

    /// Get who created this entity
    fn created_by(&self) -> Option<&Self::Actor>;

    /// Get who last modified this entity
    fn modified_by(&self) -> Option<&Self::Actor>;

    /// Set who is modifying this entity
    fn set_modified_by(&mut self, actor: Self::Actor);
}

/// Trait for entities that can be serialized to different formats
#[async_trait]
pub trait SerializablePersistable: Persistable {
    /// Serialize entity to JSON
    fn to_json(&self) -> OrcherResult<String> {
        serde_json::to_string(self).map_err(|e| {
            crate::error::StorageError::ValidationFailed {
                field: "serialization".to_string(),
                message: format!("JSON serialization failed: {}", e),
            }
            .into()
        })
    }

    /// Deserialize entity from JSON
    fn from_json(json: &str) -> OrcherResult<Self> {
        serde_json::from_str(json).map_err(|e| {
            crate::error::StorageError::ValidationFailed {
                field: "deserialization".to_string(),
                message: format!("JSON deserialization failed: {}", e),
            }
            .into()
        })
    }

    /// Serialize entity to binary format (MessagePack)
    async fn to_binary(&self) -> OrcherResult<Vec<u8>> {
        rmp_serde::to_vec(self).map_err(|e| {
            crate::error::StorageError::ValidationFailed {
                field: "serialization".to_string(),
                message: format!("Binary serialization failed: {}", e),
            }
            .into()
        })
    }

    /// Deserialize entity from binary format (MessagePack)
    async fn from_binary(data: &[u8]) -> OrcherResult<Self> {
        rmp_serde::from_slice(data).map_err(|e| {
            crate::error::StorageError::ValidationFailed {
                field: "deserialization".to_string(),
                message: format!("Binary deserialization failed: {}", e),
            }
            .into()
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use orcher_common::{
        types::identifiers::{traits::Identifier, TaskId},
        validation::{Validatable, ValidationResult},
    };
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct TestEntity {
        id: TaskId,
        name: String,
        created_at: Timestamptz,
        updated_at: Timestamptz,
        version: Option<i64>,
        is_new: bool,
    }

    impl TestEntity {
        fn new(name: String) -> Self {
            let now = Timestamptz::now();
            Self {
                id: TaskId::new_v4(),
                name,
                created_at: now,
                updated_at: now,
                version: Some(1),
                is_new: true,
            }
        }
    }

    impl Validatable for TestEntity {
        fn validate(&self) -> ValidationResult {
            let mut result = ValidationResult::new();
            if self.name.trim().is_empty() {
                result.add_error("name", "Name cannot be empty");
            }
            result
        }
    }

    impl Persistable for TestEntity {
        type Id = TaskId;

        fn id(&self) -> &Self::Id {
            &self.id
        }

        fn object_type() -> &'static str {
            "test_entity"
        }

        fn is_new(&self) -> bool {
            self.is_new
        }

        fn mark_persisted(&mut self) {
            self.is_new = false;
        }

        fn version(&self) -> Option<i64> {
            self.version
        }

        fn set_version(&mut self, version: i64) {
            self.version = Some(version);
        }
    }

    impl Timestamped for TestEntity {
        fn created_at(&self) -> Timestamptz {
            self.created_at
        }

        fn updated_at(&self) -> Timestamptz {
            self.updated_at
        }

        fn touch(&mut self) {
            self.updated_at = Timestamptz::now();
        }
    }

    impl SerializablePersistable for TestEntity {}

    #[test]
    fn test_persistable_entity_creation() {
        let entity = TestEntity::new("test".to_string());
        assert!(entity.id().is_valid());
        assert_eq!(TestEntity::object_type(), "test_entity");
        assert_eq!(TestEntity::table_name(), "test_entity");
        assert_eq!(TestEntity::primary_key(), "id");
        assert!(entity.is_new());
    }

    #[test]
    fn test_persistable_validation() {
        let mut valid_entity = TestEntity::new("valid".to_string());
        assert!(valid_entity.before_persist().is_ok());

        let mut invalid_entity = TestEntity::new("".to_string());
        assert!(invalid_entity.before_persist().is_err());
    }

    #[test]
    fn test_persistable_lifecycle() {
        let mut entity = TestEntity::new("test".to_string());
        assert!(entity.is_new());

        entity.after_persist().unwrap();
        assert!(!entity.is_new());
    }

    #[test]
    fn test_timestamped_functionality() {
        let mut entity = TestEntity::new("test".to_string());
        let original_updated = entity.updated_at();

        // Sleep a tiny bit to ensure timestamp difference
        std::thread::sleep(std::time::Duration::from_millis(1));
        entity.touch();

        assert!(entity.updated_at() > original_updated);
        assert_eq!(entity.created_at(), entity.created_at); // Should not change
    }

    #[test]
    fn test_serializable_persistable() {
        let entity = TestEntity::new("test".to_string());

        // Test JSON serialization
        let json = entity.to_json().unwrap();
        let deserialized = TestEntity::from_json(&json).unwrap();
        assert_eq!(entity.id(), deserialized.id());
        assert_eq!(entity.name, deserialized.name);
    }

    #[tokio::test]
    async fn test_binary_serialization() {
        let entity = TestEntity::new("test".to_string());

        // Test binary serialization
        let binary = entity.to_binary().await.unwrap();
        let deserialized = TestEntity::from_binary(&binary).await.unwrap();
        assert_eq!(entity.id(), deserialized.id());
        assert_eq!(entity.name, deserialized.name);
    }

    #[test]
    fn test_version_management() {
        let mut entity = TestEntity::new("test".to_string());
        assert_eq!(entity.version(), Some(1));

        entity.set_version(5);
        assert_eq!(entity.version(), Some(5));
    }
}
