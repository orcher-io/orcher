//! Thin wrapper around a tokio broadcast channel with a fixed event type.

use crate::error::Result;
use std::fmt;
use tokio::sync::broadcast;

/// A single broadcast channel scoped to one event type.
pub struct TypedChannel<E>
where
    E: Clone + Send + Sync + 'static,
{
    sender: broadcast::Sender<E>,
    capacity: usize,
}

impl<E> TypedChannel<E>
where
    E: Clone + Send + Sync + 'static,
{
    /// Create a channel that can buffer up to `capacity` events.
    pub fn new(capacity: usize) -> Self {
        let (sender, _) = broadcast::channel(capacity);
        Self { sender, capacity }
    }

    /// Send an event to every active subscriber. Silently succeeds if nobody is listening.
    pub async fn publish(&self, event: E) -> Result<()> {
        // Send returns the number of receivers, or Err if there are none.
        // We don't consider "no receivers" an error for our use case.
        match self.sender.send(event) {
            Ok(_) => Ok(()),
            Err(_) => Ok(()), // No receivers - not an error
        }
    }

    /// Get a new receiver. Only sees events published *after* this call.
    pub fn subscribe(&self) -> broadcast::Receiver<E> {
        self.sender.subscribe()
    }

    /// How many receivers are currently alive.
    pub fn subscriber_count(&self) -> usize {
        self.sender.receiver_count()
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    pub fn has_subscribers(&self) -> bool {
        self.subscriber_count() > 0
    }
}

impl<E> Clone for TypedChannel<E>
where
    E: Clone + Send + Sync + 'static,
{
    fn clone(&self) -> Self {
        Self {
            sender: self.sender.clone(),
            capacity: self.capacity,
        }
    }
}

impl<E> fmt::Debug for TypedChannel<E>
where
    E: Clone + Send + Sync + 'static,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TypedChannel")
            .field("capacity", &self.capacity)
            .field("subscriber_count", &self.subscriber_count())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::TaskEvent;
    use orcher_common::types::identifiers::{Identifier, TaskId};
    use orcher_common::types::time::Timestamptz;

    #[test]
    fn test_channel_creation() {
        let channel = TypedChannel::<TaskEvent>::new(1000);
        assert_eq!(channel.capacity(), 1000);
        assert_eq!(channel.subscriber_count(), 0);
        assert!(!channel.has_subscribers());
    }

    #[test]
    fn test_subscriber_count() {
        let channel = TypedChannel::<TaskEvent>::new(1000);
        assert_eq!(channel.subscriber_count(), 0);

        let _receiver1 = channel.subscribe();
        assert_eq!(channel.subscriber_count(), 1);

        let _receiver2 = channel.subscribe();
        assert_eq!(channel.subscriber_count(), 2);

        drop(_receiver1);
        assert_eq!(channel.subscriber_count(), 1);

        drop(_receiver2);
        assert_eq!(channel.subscriber_count(), 0);
    }

    #[test]
    fn test_has_subscribers() {
        let channel = TypedChannel::<TaskEvent>::new(1000);
        assert!(!channel.has_subscribers());

        let _receiver = channel.subscribe();
        assert!(channel.has_subscribers());

        drop(_receiver);
        assert!(!channel.has_subscribers());
    }

    #[tokio::test]
    async fn test_publish_subscribe() {
        let channel = TypedChannel::<TaskEvent>::new(1000);
        let mut receiver = channel.subscribe();

        let task_id = TaskId::generate();
        let event = TaskEvent::Submitted {
            task_id,
            submitted_at: Timestamptz::now(),
            priority: 5,
        };

        channel.publish(event.clone()).await.unwrap();

        let received = receiver.recv().await.unwrap();
        match received {
            TaskEvent::Submitted {
                task_id: id,
                priority,
                ..
            } => {
                assert_eq!(id, task_id);
                assert_eq!(priority, 5);
            }
            _ => panic!("Expected TaskEvent::Submitted"),
        }
    }

    #[tokio::test]
    async fn test_multiple_subscribers() {
        let channel = TypedChannel::<TaskEvent>::new(1000);
        let mut receiver1 = channel.subscribe();
        let mut receiver2 = channel.subscribe();

        let task_id = TaskId::generate();
        let event = TaskEvent::Submitted {
            task_id,
            submitted_at: Timestamptz::now(),
            priority: 5,
        };

        channel.publish(event.clone()).await.unwrap();

        // Both receivers should get the same event
        let received1 = receiver1.recv().await.unwrap();
        let received2 = receiver2.recv().await.unwrap();

        match (received1, received2) {
            (
                TaskEvent::Submitted {
                    task_id: id1,
                    priority: p1,
                    ..
                },
                TaskEvent::Submitted {
                    task_id: id2,
                    priority: p2,
                    ..
                },
            ) => {
                assert_eq!(id1, task_id);
                assert_eq!(id2, task_id);
                assert_eq!(p1, 5);
                assert_eq!(p2, 5);
            }
            _ => panic!("Expected TaskEvent::Submitted"),
        }
    }

    #[tokio::test]
    async fn test_publish_without_subscribers() {
        let channel = TypedChannel::<TaskEvent>::new(1000);

        let task_id = TaskId::generate();
        let event = TaskEvent::Submitted {
            task_id,
            submitted_at: Timestamptz::now(),
            priority: 5,
        };

        // Should not error even with no subscribers
        let result = channel.publish(event).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_late_subscription() {
        let channel = TypedChannel::<TaskEvent>::new(1000);

        let task_id1 = TaskId::generate();
        let event1 = TaskEvent::Submitted {
            task_id: task_id1,
            submitted_at: Timestamptz::now(),
            priority: 5,
        };

        // Publish before subscription
        channel.publish(event1).await.unwrap();

        // Late subscriber should NOT receive the event
        let mut receiver = channel.subscribe();

        let task_id2 = TaskId::generate();
        let event2 = TaskEvent::Submitted {
            task_id: task_id2,
            submitted_at: Timestamptz::now(),
            priority: 10,
        };

        // Publish after subscription
        channel.publish(event2).await.unwrap();

        // Should only receive the second event
        let received = receiver.recv().await.unwrap();
        match received {
            TaskEvent::Submitted { task_id, .. } => {
                assert_eq!(task_id, task_id2);
            }
            _ => panic!("Expected TaskEvent::Submitted"),
        }
    }

    #[test]
    fn test_channel_clone() {
        let channel1 = TypedChannel::<TaskEvent>::new(1000);
        let channel2 = channel1.clone();

        assert_eq!(channel1.capacity(), channel2.capacity());

        let _receiver = channel1.subscribe();
        assert_eq!(channel2.subscriber_count(), 1);
    }

    #[test]
    fn test_channel_debug() {
        let channel = TypedChannel::<TaskEvent>::new(5000);
        let debug_str = format!("{:?}", channel);
        assert!(debug_str.contains("TypedChannel"));
        assert!(debug_str.contains("capacity"));
        assert!(debug_str.contains("5000"));
    }
}
