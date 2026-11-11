//! # Mock Repository Implementations
//!
//! This module provides mock implementations of storage repositories
//! for testing purposes, using orcher-common types.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use orcher_common::OrcherResult;

/// Generic mock repository for testing
#[derive(Debug, Clone)]
pub struct MockRepository<T> {
    storage: Arc<RwLock<HashMap<String, T>>>,
}

impl<T> Default for MockRepository<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> MockRepository<T> {
    /// Create a new mock repository
    pub fn new() -> Self {
        Self {
            storage: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Insert an item into the mock storage
    pub fn insert(&self, key: String, value: T) -> OrcherResult<()> {
        let mut storage = self.storage.write().unwrap();
        storage.insert(key, value);
        Ok(())
    }

    /// Get an item from the mock storage
    pub fn get(&self, key: &str) -> Option<T>
    where
        T: Clone,
    {
        let storage = self.storage.read().unwrap();
        storage.get(key).cloned()
    }

    /// Remove an item from the mock storage
    pub fn remove(&self, key: &str) -> Option<T> {
        let mut storage = self.storage.write().unwrap();
        storage.remove(key)
    }

    /// Check if a key exists
    pub fn contains_key(&self, key: &str) -> bool {
        let storage = self.storage.read().unwrap();
        storage.contains_key(key)
    }

    /// Clear all items from the mock storage
    pub fn clear(&self) {
        let mut storage = self.storage.write().unwrap();
        storage.clear();
    }

    /// Get the number of items in the mock storage
    pub fn len(&self) -> usize {
        let storage = self.storage.read().unwrap();
        storage.len()
    }

    /// Check if the mock storage is empty
    pub fn is_empty(&self) -> bool {
        let storage = self.storage.read().unwrap();
        storage.is_empty()
    }

    /// Get all keys in the storage
    pub fn keys(&self) -> Vec<String> {
        let storage = self.storage.read().unwrap();
        storage.keys().cloned().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mock_repository_insert_and_get() {
        let repo = MockRepository::new();
        repo.insert("key1".to_string(), "value1".to_string())
            .unwrap();

        assert_eq!(repo.get("key1"), Some("value1".to_string()));
        assert_eq!(repo.len(), 1);
        assert!(repo.contains_key("key1"));
    }

    #[test]
    fn test_mock_repository_remove() {
        let repo = MockRepository::new();
        repo.insert("key1".to_string(), "value1".to_string())
            .unwrap();

        let removed = repo.remove("key1");
        assert_eq!(removed, Some("value1".to_string()));
        assert!(repo.is_empty());
        assert!(!repo.contains_key("key1"));
    }

    #[test]
    fn test_mock_repository_clear() {
        let repo = MockRepository::new();
        repo.insert("key1".to_string(), "value1".to_string())
            .unwrap();
        repo.insert("key2".to_string(), "value2".to_string())
            .unwrap();

        assert_eq!(repo.len(), 2);
        repo.clear();
        assert!(repo.is_empty());
    }

    #[test]
    fn test_mock_repository_keys() {
        let repo = MockRepository::new();
        repo.insert("key1".to_string(), "value1".to_string())
            .unwrap();
        repo.insert("key2".to_string(), "value2".to_string())
            .unwrap();

        let keys = repo.keys();
        assert_eq!(keys.len(), 2);
        assert!(keys.contains(&"key1".to_string()));
        assert!(keys.contains(&"key2".to_string()));
    }
}
