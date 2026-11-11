//! # Cacheable Trait
//!
//! Core trait for cache strategy definitions using orcher-common types
//! for maximum compatibility and type safety across the ORCHER ecosystem.

use async_trait::async_trait;
use orcher_common::types::time::Timestamptz;
use orcher_common::OrcherResult;
use serde::{Deserialize, Serialize};
use std::time::Duration;

use super::persistable::Persistable;

/// Core trait for entities that can be cached
///
/// This trait defines the caching contract for entities in the ORCHER storage system,
/// including TTL management, cache key generation, and invalidation strategies.
pub trait Cacheable: Persistable {
    /// Generate cache key for this entity
    fn cache_key(&self) -> String {
        format!("{}:{}", Self::object_type(), self.id())
    }

    /// Get cache namespace for this entity type
    fn cache_namespace() -> &'static str {
        Self::object_type()
    }

    /// Get Time-To-Live for cached entity (default: 1 hour)
    fn cache_ttl(&self) -> Duration {
        Duration::from_secs(3600) // 1 hour default
    }

    /// Get cache priority (higher values = higher priority)
    fn cache_priority(&self) -> u8 {
        100 // Default priority
    }

    /// Check if entity should be cached based on current state
    fn should_cache(&self) -> bool {
        true // Default: cache everything
    }

    /// Get cache tags for invalidation grouping
    fn cache_tags(&self) -> Vec<String> {
        vec![Self::cache_namespace().to_string()]
    }

    /// Generate cache key for entity by ID
    fn cache_key_for_id(id: &Self::Id) -> String {
        format!("{}:{}", Self::object_type(), id)
    }

    /// Generate cache key pattern for bulk operations
    fn cache_key_pattern() -> String {
        format!("{}:*", Self::object_type())
    }
}

/// Cache invalidation strategy
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum CacheInvalidationStrategy {
    /// Immediate invalidation
    Immediate,
    /// Lazy invalidation (mark as stale)
    Lazy,
    /// Time-based invalidation
    TimeBased {
        /// Duration after which to invalidate
        after: Duration,
    },
    /// Event-based invalidation
    EventBased {
        /// Events that trigger invalidation
        events: Vec<String>,
    },
    /// Custom invalidation logic
    Custom {
        /// Custom strategy identifier
        strategy: String,
    },
}

/// Cache storage tier
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum CacheTier {
    /// L1 cache (in-memory)
    Memory,
    /// L2 cache (Redis)
    Redis,
    /// L3 cache (disk-based)
    Disk,
    /// Distributed cache
    Distributed,
}

/// Cache entry metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheMetadata {
    /// When the entry was cached
    pub cached_at: Timestamptz,
    /// When the entry expires
    pub expires_at: Timestamptz,
    /// Cache hit count
    pub hit_count: u64,
    /// Last accessed timestamp
    pub last_accessed: Timestamptz,
    /// Cache tier where entry is stored
    pub tier: CacheTier,
    /// Cache tags for invalidation
    pub tags: Vec<String>,
    /// Cache priority
    pub priority: u8,
    /// Entity version when cached
    pub entity_version: Option<i64>,
}

/// Advanced caching trait with multi-tier support
#[async_trait]
pub trait AdvancedCacheable: Cacheable {
    /// Get preferred cache tier for this entity
    fn preferred_cache_tier(&self) -> CacheTier {
        CacheTier::Redis
    }

    /// Get cache invalidation strategy
    fn invalidation_strategy(&self) -> CacheInvalidationStrategy {
        CacheInvalidationStrategy::Immediate
    }

    /// Determine if entity should be promoted to higher cache tier
    async fn should_promote(&self, metadata: &CacheMetadata) -> bool {
        // Promote frequently accessed items
        metadata.hit_count > 10
    }

    /// Determine if entity should be demoted to lower cache tier
    async fn should_demote(&self, metadata: &CacheMetadata) -> bool {
        // Demote rarely accessed items
        let age = Timestamptz::now().signed_duration_since(metadata.last_accessed);
        age.as_hours() >= 24 && metadata.hit_count < 5
    }

    /// Custom cache key generation logic
    fn custom_cache_key(&self, context: &str) -> String {
        format!("{}:{}:{}", context, Self::object_type(), self.id())
    }

    /// Prepare entity for caching (compress, encrypt, etc.)
    async fn prepare_for_cache(&self) -> OrcherResult<Vec<u8>> {
        // Default: serialize to JSON and convert to bytes
        let json = serde_json::to_string(self).map_err(|e| {
            let storage_error = crate::error::StorageError::ValidationFailed {
                field: "cache_serialization".to_string(),
                message: format!("Failed to serialize for cache: {}", e),
            };
            Into::<orcher_common::error::OrcherError>::into(storage_error)
        })?;
        Ok(json.into_bytes())
    }

    /// Restore entity from cache (decompress, decrypt, etc.)
    async fn restore_from_cache(data: &[u8]) -> OrcherResult<Self> {
        // Default: deserialize from JSON bytes
        let json = String::from_utf8(data.to_vec()).map_err(|e| {
            let storage_error = crate::error::StorageError::ValidationFailed {
                field: "cache_deserialization".to_string(),
                message: format!("Failed to convert cache data to string: {}", e),
            };
            Into::<orcher_common::error::OrcherError>::into(storage_error)
        })?;
        serde_json::from_str(&json).map_err(|e| {
            let storage_error = crate::error::StorageError::ValidationFailed {
                field: "cache_deserialization".to_string(),
                message: format!("Failed to deserialize from cache: {}", e),
            };
            Into::<orcher_common::error::OrcherError>::into(storage_error)
        })
    }
}

/// Trait for cache-aware collections
pub trait CacheableCollection: Cacheable {
    /// Generate cache key for collection queries
    fn collection_cache_key(query_hash: &str) -> String {
        format!("{}:collection:{}", Self::object_type(), query_hash)
    }

    /// Get cache TTL for collection queries
    fn collection_cache_ttl() -> Duration {
        Duration::from_secs(600) // 10 minutes for collections
    }

    /// Generate cache tags for collection invalidation
    fn collection_cache_tags() -> Vec<String> {
        vec![
            format!("{}_collection", Self::object_type()),
            Self::cache_namespace().to_string(),
        ]
    }
}

/// Cache statistics for monitoring
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheStats {
    /// Total cache hits
    pub hits: u64,
    /// Total cache misses
    pub misses: u64,
    /// Cache hit ratio (0.0 to 1.0)
    pub hit_ratio: f64,
    /// Total cached entities
    pub total_entries: u64,
    /// Total cache memory usage in bytes
    pub memory_usage: u64,
    /// Average TTL across all entries
    pub average_ttl_seconds: u64,
}

impl CacheStats {
    /// Create new cache statistics
    pub fn new() -> Self {
        Self {
            hits: 0,
            misses: 0,
            hit_ratio: 0.0,
            total_entries: 0,
            memory_usage: 0,
            average_ttl_seconds: 0,
        }
    }

    /// Update hit ratio based on hits and misses
    pub fn update_hit_ratio(&mut self) {
        let total = self.hits + self.misses;
        if total > 0 {
            self.hit_ratio = self.hits as f64 / total as f64;
        }
    }

    /// Record a cache hit
    pub fn record_hit(&mut self) {
        self.hits += 1;
        self.update_hit_ratio();
    }

    /// Record a cache miss
    pub fn record_miss(&mut self) {
        self.misses += 1;
        self.update_hit_ratio();
    }
}

impl Default for CacheStats {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use orcher_common::types::time::Timestamptz;
    use orcher_common::{
        types::identifiers::TaskId,
        validation::{Validatable, ValidationResult},
    };
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct TestCacheableEntity {
        id: TaskId,
        name: String,
        priority: u8,
    }

    impl TestCacheableEntity {
        fn new(name: String, priority: u8) -> Self {
            Self {
                id: TaskId::new_v4(),
                name,
                priority,
            }
        }
    }

    impl Validatable for TestCacheableEntity {
        fn validate(&self) -> ValidationResult {
            ValidationResult::new()
        }
    }

    impl Persistable for TestCacheableEntity {
        type Id = TaskId;

        fn id(&self) -> &Self::Id {
            &self.id
        }

        fn object_type() -> &'static str {
            "test_cacheable"
        }
    }

    impl Cacheable for TestCacheableEntity {
        fn cache_ttl(&self) -> Duration {
            Duration::from_secs(1800) // 30 minutes
        }

        fn cache_priority(&self) -> u8 {
            self.priority
        }

        fn should_cache(&self) -> bool {
            self.priority > 50 // Only cache high priority items
        }

        fn cache_tags(&self) -> Vec<String> {
            vec![
                Self::cache_namespace().to_string(),
                format!("priority_{}", self.priority),
            ]
        }
    }

    impl AdvancedCacheable for TestCacheableEntity {
        fn preferred_cache_tier(&self) -> CacheTier {
            if self.priority > 150 {
                CacheTier::Memory
            } else {
                CacheTier::Redis
            }
        }

        fn invalidation_strategy(&self) -> CacheInvalidationStrategy {
            CacheInvalidationStrategy::EventBased {
                events: vec![
                    "entity_updated".to_string(),
                    "high_priority_changed".to_string(),
                ],
            }
        }
    }

    impl CacheableCollection for TestCacheableEntity {}

    #[test]
    fn test_cacheable_basic_functionality() {
        let entity = TestCacheableEntity::new("test".to_string(), 100);

        assert_eq!(TestCacheableEntity::cache_namespace(), "test_cacheable");
        assert!(entity.cache_key().contains("test_cacheable"));
        assert!(entity.cache_key().contains(&format!("{}", entity.id())));
        assert_eq!(entity.cache_ttl(), Duration::from_secs(1800));
        assert_eq!(entity.cache_priority(), 100);
        assert!(entity.should_cache()); // Priority > 50
    }

    #[test]
    fn test_cacheable_low_priority() {
        let entity = TestCacheableEntity::new("test".to_string(), 30);
        assert!(!entity.should_cache()); // Priority <= 50
    }

    #[test]
    fn test_cache_key_generation() {
        let entity = TestCacheableEntity::new("test".to_string(), 100);
        let expected_prefix = "test_cacheable:";
        assert!(entity.cache_key().starts_with(expected_prefix));

        let key_for_id = TestCacheableEntity::cache_key_for_id(entity.id());
        assert_eq!(entity.cache_key(), key_for_id);

        let pattern = TestCacheableEntity::cache_key_pattern();
        assert_eq!(pattern, "test_cacheable:*");
    }

    #[test]
    fn test_cache_tags() {
        let entity = TestCacheableEntity::new("test".to_string(), 100);
        let tags = entity.cache_tags();
        assert!(tags.contains(&"test_cacheable".to_string()));
        assert!(tags.contains(&"priority_100".to_string()));
    }

    #[test]
    fn test_advanced_cacheable() {
        let high_priority_entity = TestCacheableEntity::new("high".to_string(), 200);
        let low_priority_entity = TestCacheableEntity::new("low".to_string(), 80);

        assert_eq!(
            high_priority_entity.preferred_cache_tier(),
            CacheTier::Memory
        );
        assert_eq!(low_priority_entity.preferred_cache_tier(), CacheTier::Redis);

        let strategy = high_priority_entity.invalidation_strategy();
        if let CacheInvalidationStrategy::EventBased { events } = strategy {
            assert!(events.contains(&"entity_updated".to_string()));
        } else {
            panic!("Expected EventBased invalidation strategy");
        }
    }

    #[test]
    fn test_cacheable_collection() {
        let key = TestCacheableEntity::collection_cache_key("query123");
        assert_eq!(key, "test_cacheable:collection:query123");

        let ttl = TestCacheableEntity::collection_cache_ttl();
        assert_eq!(ttl, Duration::from_secs(600));

        let tags = TestCacheableEntity::collection_cache_tags();
        assert!(tags.contains(&"test_cacheable_collection".to_string()));
        assert!(tags.contains(&"test_cacheable".to_string()));
    }

    #[tokio::test]
    async fn test_cache_preparation() {
        let entity = TestCacheableEntity::new("test".to_string(), 100);

        let cached_data = entity.prepare_for_cache().await.unwrap();
        assert!(!cached_data.is_empty());

        let restored = TestCacheableEntity::restore_from_cache(&cached_data)
            .await
            .unwrap();
        assert_eq!(entity.id(), restored.id());
        assert_eq!(entity.name, restored.name);
        assert_eq!(entity.priority, restored.priority);
    }

    #[tokio::test]
    async fn test_cache_metadata_promotion_demotion() {
        let entity = TestCacheableEntity::new("test".to_string(), 100);
        let now = Timestamptz::now();

        // Test promotion (high hit count)
        let high_hit_metadata = CacheMetadata {
            cached_at: now,
            expires_at: now + jiff::SignedDuration::from_hours(1),
            hit_count: 15,
            last_accessed: now,
            tier: CacheTier::Redis,
            tags: vec![],
            priority: 100,
            entity_version: Some(1),
        };
        assert!(entity.should_promote(&high_hit_metadata).await);

        // Test demotion (old and low hit count)
        let old_low_hit_metadata = CacheMetadata {
            cached_at: now - jiff::SignedDuration::from_hours(2),
            expires_at: now + jiff::SignedDuration::from_hours(1),
            hit_count: 2,
            last_accessed: now - jiff::SignedDuration::from_hours(25), // More than 24 hours ago
            tier: CacheTier::Memory,
            tags: vec![],
            priority: 100,
            entity_version: Some(1),
        };
        assert!(entity.should_demote(&old_low_hit_metadata).await);
    }

    #[test]
    fn test_cache_stats() {
        let mut stats = CacheStats::new();
        assert_eq!(stats.hits, 0);
        assert_eq!(stats.misses, 0);
        assert_eq!(stats.hit_ratio, 0.0);

        stats.record_hit();
        stats.record_hit();
        stats.record_miss();

        assert_eq!(stats.hits, 2);
        assert_eq!(stats.misses, 1);
        assert!((stats.hit_ratio - 0.6666666666666666).abs() < f64::EPSILON);
    }

    #[test]
    fn test_cache_invalidation_strategy_serialization() {
        let strategies = vec![
            CacheInvalidationStrategy::Immediate,
            CacheInvalidationStrategy::Lazy,
            CacheInvalidationStrategy::TimeBased {
                after: Duration::from_secs(60),
            },
            CacheInvalidationStrategy::EventBased {
                events: vec!["test".to_string()],
            },
            CacheInvalidationStrategy::Custom {
                strategy: "custom_logic".to_string(),
            },
        ];

        for strategy in strategies {
            let serialized = serde_json::to_string(&strategy).unwrap();
            let deserialized: CacheInvalidationStrategy =
                serde_json::from_str(&serialized).unwrap();
            assert_eq!(strategy, deserialized);
        }
    }
}
