//! Event subscription management.

use orcher_common::types::time::Timestamptz;
use orcher_common::OrcherResult;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Handle representing an active subscription
#[derive(Debug, Clone)]
pub struct SubscriptionHandle {
    /// Unique subscription identifier
    pub id: String,
    /// Type of events subscribed to (e.g., "TaskEvent", "WorkerEvent")
    pub event_type: String,
    /// Name of the subscribing component
    pub subscriber: String,
    /// When the subscription was created
    pub created_at: Timestamptz,
}

/// Manager for tracking active event subscriptions
///
/// Maintains a registry of all active subscriptions for monitoring
/// and cleanup purposes.
pub struct SubscriptionManager {
    subscriptions: Arc<RwLock<HashMap<String, SubscriptionHandle>>>,
}

impl SubscriptionManager {
    /// Create a new subscription manager
    pub fn new() -> Self {
        Self {
            subscriptions: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Register a new subscription
    ///
    /// # Arguments
    /// * `handle` - The subscription handle to register
    ///
    /// # Returns
    /// Ok(()) on success
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use orcher_core::events::{SubscriptionManager, SubscriptionHandle};
    /// # use orcher_common::types::time::Timestamptz;
    /// # async fn example() -> orcher_common::OrcherResult<()> {
    /// let manager = SubscriptionManager::new();
    /// let handle = SubscriptionHandle {
    ///     id: "sub-1".to_string(),
    ///     event_type: "TaskEvent".to_string(),
    ///     subscriber: "TaskAssigner".to_string(),
    ///     created_at: Timestamptz::now(),
    /// };
    /// manager.register(handle).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn register(&self, handle: SubscriptionHandle) -> OrcherResult<()> {
        let mut subs = self.subscriptions.write().await;
        subs.insert(handle.id.clone(), handle);
        Ok(())
    }

    /// Unregister a subscription
    ///
    /// # Arguments
    /// * `id` - The subscription ID to unregister
    ///
    /// # Returns
    /// Ok(()) on success, even if subscription doesn't exist
    pub async fn unregister(&self, id: &str) -> OrcherResult<()> {
        let mut subs = self.subscriptions.write().await;
        subs.remove(id);
        Ok(())
    }

    /// Get a list of all active subscriptions
    ///
    /// # Returns
    /// Vector of active subscription handles
    pub async fn list_active(&self) -> Vec<SubscriptionHandle> {
        let subs = self.subscriptions.read().await;
        subs.values().cloned().collect()
    }

    /// Get a subscription by ID
    ///
    /// # Arguments
    /// * `id` - The subscription ID to retrieve
    ///
    /// # Returns
    /// Some(handle) if found, None otherwise
    pub async fn get(&self, id: &str) -> Option<SubscriptionHandle> {
        let subs = self.subscriptions.read().await;
        subs.get(id).cloned()
    }

    /// Get all subscriptions for a specific event type
    ///
    /// # Arguments
    /// * `event_type` - The event type to filter by
    ///
    /// # Returns
    /// Vector of matching subscription handles
    pub async fn list_by_event_type(&self, event_type: &str) -> Vec<SubscriptionHandle> {
        let subs = self.subscriptions.read().await;
        subs.values()
            .filter(|h| h.event_type == event_type)
            .cloned()
            .collect()
    }

    /// Get all subscriptions for a specific subscriber
    ///
    /// # Arguments
    /// * `subscriber` - The subscriber name to filter by
    ///
    /// # Returns
    /// Vector of matching subscription handles
    pub async fn list_by_subscriber(&self, subscriber: &str) -> Vec<SubscriptionHandle> {
        let subs = self.subscriptions.read().await;
        subs.values()
            .filter(|h| h.subscriber == subscriber)
            .cloned()
            .collect()
    }

    /// Get the total number of active subscriptions
    pub async fn count(&self) -> usize {
        let subs = self.subscriptions.read().await;
        subs.len()
    }

    /// Clear all subscriptions
    pub async fn clear(&self) {
        let mut subs = self.subscriptions.write().await;
        subs.clear();
    }
}

impl Default for SubscriptionManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_subscription_manager_creation() {
        let manager = SubscriptionManager::new();
        assert_eq!(manager.count().await, 0);
    }

    #[tokio::test]
    async fn test_register_subscription() {
        let manager = SubscriptionManager::new();
        let handle = SubscriptionHandle {
            id: "sub-1".to_string(),
            event_type: "TaskEvent".to_string(),
            subscriber: "TaskAssigner".to_string(),
            created_at: Timestamptz::now(),
        };

        manager.register(handle.clone()).await.unwrap();
        assert_eq!(manager.count().await, 1);

        let retrieved = manager.get("sub-1").await;
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().id, "sub-1");
    }

    #[tokio::test]
    async fn test_unregister_subscription() {
        let manager = SubscriptionManager::new();
        let handle = SubscriptionHandle {
            id: "sub-1".to_string(),
            event_type: "TaskEvent".to_string(),
            subscriber: "TaskAssigner".to_string(),
            created_at: Timestamptz::now(),
        };

        manager.register(handle).await.unwrap();
        assert_eq!(manager.count().await, 1);

        manager.unregister("sub-1").await.unwrap();
        assert_eq!(manager.count().await, 0);
    }

    #[tokio::test]
    async fn test_list_active_subscriptions() {
        let manager = SubscriptionManager::new();

        let handle1 = SubscriptionHandle {
            id: "sub-1".to_string(),
            event_type: "TaskEvent".to_string(),
            subscriber: "TaskAssigner".to_string(),
            created_at: Timestamptz::now(),
        };

        let handle2 = SubscriptionHandle {
            id: "sub-2".to_string(),
            event_type: "WorkerEvent".to_string(),
            subscriber: "HealthMonitor".to_string(),
            created_at: Timestamptz::now(),
        };

        manager.register(handle1).await.unwrap();
        manager.register(handle2).await.unwrap();

        let active = manager.list_active().await;
        assert_eq!(active.len(), 2);
    }

    #[tokio::test]
    async fn test_list_by_event_type() {
        let manager = SubscriptionManager::new();

        let handle1 = SubscriptionHandle {
            id: "sub-1".to_string(),
            event_type: "TaskEvent".to_string(),
            subscriber: "TaskAssigner".to_string(),
            created_at: Timestamptz::now(),
        };

        let handle2 = SubscriptionHandle {
            id: "sub-2".to_string(),
            event_type: "TaskEvent".to_string(),
            subscriber: "TaskProcessor".to_string(),
            created_at: Timestamptz::now(),
        };

        let handle3 = SubscriptionHandle {
            id: "sub-3".to_string(),
            event_type: "WorkerEvent".to_string(),
            subscriber: "HealthMonitor".to_string(),
            created_at: Timestamptz::now(),
        };

        manager.register(handle1).await.unwrap();
        manager.register(handle2).await.unwrap();
        manager.register(handle3).await.unwrap();

        let task_subs = manager.list_by_event_type("TaskEvent").await;
        assert_eq!(task_subs.len(), 2);

        let worker_subs = manager.list_by_event_type("WorkerEvent").await;
        assert_eq!(worker_subs.len(), 1);
    }

    #[tokio::test]
    async fn test_list_by_subscriber() {
        let manager = SubscriptionManager::new();

        let handle1 = SubscriptionHandle {
            id: "sub-1".to_string(),
            event_type: "TaskEvent".to_string(),
            subscriber: "TaskAssigner".to_string(),
            created_at: Timestamptz::now(),
        };

        let handle2 = SubscriptionHandle {
            id: "sub-2".to_string(),
            event_type: "WorkerEvent".to_string(),
            subscriber: "TaskAssigner".to_string(),
            created_at: Timestamptz::now(),
        };

        manager.register(handle1).await.unwrap();
        manager.register(handle2).await.unwrap();

        let subs = manager.list_by_subscriber("TaskAssigner").await;
        assert_eq!(subs.len(), 2);
    }

    #[tokio::test]
    async fn test_clear_subscriptions() {
        let manager = SubscriptionManager::new();

        let handle = SubscriptionHandle {
            id: "sub-1".to_string(),
            event_type: "TaskEvent".to_string(),
            subscriber: "TaskAssigner".to_string(),
            created_at: Timestamptz::now(),
        };

        manager.register(handle).await.unwrap();
        assert_eq!(manager.count().await, 1);

        manager.clear().await;
        assert_eq!(manager.count().await, 0);
    }

    #[tokio::test]
    async fn test_unregister_nonexistent_succeeds() {
        let manager = SubscriptionManager::new();
        let result = manager.unregister("nonexistent").await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_get_nonexistent_returns_none() {
        let manager = SubscriptionManager::new();
        let result = manager.get("nonexistent").await;
        assert!(result.is_none());
    }
}
