//! Wakes long-polling services when work becomes available, keyed by `namespace:task_queue`.

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{Notify, RwLock};
use tracing::{debug, trace};

pub struct Notifier {
    channels: Arc<RwLock<HashMap<String, Arc<Notify>>>>,
}

impl Notifier {
    pub fn new() -> Self {
        Self {
            channels: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn get_or_create(&self, task_queue: &str, namespace: &str) -> Arc<Notify> {
        let queue_key = Self::make_queue_key(namespace, task_queue);

        // Fast path: read lock to check if notifier exists
        {
            let channels = self.channels.read().await;
            if let Some(notify) = channels.get(&queue_key) {
                trace!(queue_key = %queue_key, "Using existing notifier");
                return Arc::clone(notify);
            }
        }

        // Slow path: write lock to create new notifier
        let mut channels = self.channels.write().await;

        // Double-check in case another task created it while we were waiting
        if let Some(notify) = channels.get(&queue_key) {
            trace!(queue_key = %queue_key, "Notifier created by another task");
            return Arc::clone(notify);
        }

        // Create new notifier
        let notify = Arc::new(Notify::new());
        channels.insert(queue_key.clone(), Arc::clone(&notify));

        debug!(queue_key = %queue_key, "Created new notifier");
        notify
    }

    /// Wakes all pollers on this queue -- they'll race to claim via SKIP LOCKED.
    pub async fn notify_work_available(&self, task_queue: &str, namespace: &str) {
        let queue_key = Self::make_queue_key(namespace, task_queue);

        let channels = self.channels.read().await;
        if let Some(notify) = channels.get(&queue_key) {
            debug!(queue_key = %queue_key, "Notifying all waiting services");
            notify.notify_waiters();
        } else {
            trace!(queue_key = %queue_key, "No services waiting on queue");
        }
    }

    pub async fn notify_all(&self, task_queue: &str, namespace: &str) {
        let queue_key = Self::make_queue_key(namespace, task_queue);

        let channels = self.channels.read().await;
        if let Some(notify) = channels.get(&queue_key) {
            debug!(queue_key = %queue_key, "Notifying all waiting services");
            notify.notify_waiters();
        } else {
            trace!(queue_key = %queue_key, "No services waiting on queue");
        }
    }

    /// Broadcast wake to every queue -- used by the reclaimer and during shutdown.
    pub async fn notify_all_queues(&self) {
        let channels = self.channels.read().await;
        let count = channels.len();

        debug!(queue_count = count, "Notifying all queues");

        for (queue_key, notify) in channels.iter() {
            trace!(queue_key = %queue_key, "Notifying queue");
            notify.notify_waiters();
        }
    }

    pub async fn active_queue_count(&self) -> usize {
        let channels = self.channels.read().await;
        channels.len()
    }

    fn make_queue_key(namespace: &str, task_queue: &str) -> String {
        format!("{}:{}", namespace, task_queue)
    }
}

impl Default for Notifier {
    fn default() -> Self {
        Self::new()
    }
}

impl Clone for Notifier {
    fn clone(&self) -> Self {
        Self {
            channels: Arc::clone(&self.channels),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tokio::time::timeout;

    #[tokio::test]
    async fn test_notifier_creation() {
        let notifier = Notifier::new();
        let notify1 = notifier.get_or_create("test-queue", "test").await;
        let notify2 = notifier.get_or_create("test-queue", "test").await;

        // Should return the same Arc (same address)
        assert!(Arc::ptr_eq(&notify1, &notify2));
    }

    #[tokio::test]
    async fn test_different_queues_different_notifiers() {
        let notifier = Notifier::new();
        let notify1 = notifier.get_or_create("queue-1", "test").await;
        let notify2 = notifier.get_or_create("queue-2", "test").await;

        // Different queues should have different notifiers
        assert!(!Arc::ptr_eq(&notify1, &notify2));
    }

    #[tokio::test]
    async fn test_different_namespaces_different_notifiers() {
        let notifier = Notifier::new();
        let notify1 = notifier.get_or_create("test-queue", "namespace-1").await;
        let notify2 = notifier.get_or_create("test-queue", "namespace-2").await;

        // Different namespaces should have different notifiers
        assert!(!Arc::ptr_eq(&notify1, &notify2));
    }

    #[tokio::test]
    async fn test_notify_wakes_waiter() {
        let notifier = Notifier::new();
        let notify = notifier.get_or_create("test-queue", "test").await;

        // Spawn a task that waits
        let waiter = tokio::spawn({
            let notify = Arc::clone(&notify);
            async move {
                notify.notified().await;
                "woken"
            }
        });

        // Give the waiter time to start waiting
        tokio::time::sleep(Duration::from_millis(10)).await;

        // Notify
        notifier.notify_work_available("test-queue", "test").await;

        // Waiter should wake up
        let result = timeout(Duration::from_secs(1), waiter)
            .await
            .expect("Timeout waiting for task")
            .expect("Task failed");

        assert_eq!(result, "woken");
    }

    #[tokio::test]
    async fn test_notify_all_wakes_multiple_waiters() {
        let notifier = Notifier::new();
        let notify = notifier.get_or_create("test-queue", "test").await;

        // Spawn 3 waiters
        let mut waiters = Vec::new();
        for _ in 0..3 {
            let notify = Arc::clone(&notify);
            let waiter = tokio::spawn(async move {
                notify.notified().await;
                "woken"
            });
            waiters.push(waiter);
        }

        // Give waiters time to start waiting
        tokio::time::sleep(Duration::from_millis(10)).await;

        // Notify all
        notifier.notify_all("test-queue", "test").await;

        // All waiters should wake up
        for waiter in waiters {
            let result = timeout(Duration::from_secs(1), waiter)
                .await
                .expect("Timeout waiting for task")
                .expect("Task failed");

            assert_eq!(result, "woken");
        }
    }

    #[tokio::test]
    async fn test_notify_all_queues() {
        let notifier = Notifier::new();

        // Create notifiers for 3 queues
        let notify1 = notifier.get_or_create("queue-1", "test").await;
        let notify2 = notifier.get_or_create("queue-2", "test").await;
        let notify3 = notifier.get_or_create("queue-3", "test").await;

        // Spawn waiters on each queue
        let waiter1 = tokio::spawn({
            let notify = Arc::clone(&notify1);
            async move {
                notify.notified().await;
                "queue-1"
            }
        });

        let waiter2 = tokio::spawn({
            let notify = Arc::clone(&notify2);
            async move {
                notify.notified().await;
                "queue-2"
            }
        });

        let waiter3 = tokio::spawn({
            let notify = Arc::clone(&notify3);
            async move {
                notify.notified().await;
                "queue-3"
            }
        });

        // Give waiters time to start waiting
        tokio::time::sleep(Duration::from_millis(10)).await;

        // Notify all queues
        notifier.notify_all_queues().await;

        // All waiters should wake up
        assert_eq!(
            timeout(Duration::from_secs(1), waiter1)
                .await
                .unwrap()
                .unwrap(),
            "queue-1"
        );
        assert_eq!(
            timeout(Duration::from_secs(1), waiter2)
                .await
                .unwrap()
                .unwrap(),
            "queue-2"
        );
        assert_eq!(
            timeout(Duration::from_secs(1), waiter3)
                .await
                .unwrap()
                .unwrap(),
            "queue-3"
        );
    }

    #[tokio::test]
    async fn test_active_queue_count() {
        let notifier = Notifier::new();

        assert_eq!(notifier.active_queue_count().await, 0);

        notifier.get_or_create("queue-1", "test").await;
        assert_eq!(notifier.active_queue_count().await, 1);

        notifier.get_or_create("queue-2", "test").await;
        assert_eq!(notifier.active_queue_count().await, 2);

        // Getting existing notifier doesn't increase count
        notifier.get_or_create("queue-1", "test").await;
        assert_eq!(notifier.active_queue_count().await, 2);
    }

    #[tokio::test]
    async fn test_queue_key_format() {
        let key = Notifier::make_queue_key("production", "orders-queue");
        assert_eq!(key, "production:orders-queue");
    }

    #[tokio::test]
    async fn test_notifier_clone_shares_channels() {
        let notifier1 = Notifier::new();
        notifier1.get_or_create("test-queue", "test").await;

        let notifier2 = notifier1.clone();

        // Both should see the same channel
        assert_eq!(notifier1.active_queue_count().await, 1);
        assert_eq!(notifier2.active_queue_count().await, 1);

        // Notify from one should affect the other
        let notify = notifier1.get_or_create("test-queue", "test").await;
        let waiter = tokio::spawn({
            let notify = Arc::clone(&notify);
            async move {
                notify.notified().await;
                "woken"
            }
        });

        tokio::time::sleep(Duration::from_millis(10)).await;

        // Notify using the clone
        notifier2.notify_work_available("test-queue", "test").await;

        let result = timeout(Duration::from_secs(1), waiter)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(result, "woken");
    }
}
