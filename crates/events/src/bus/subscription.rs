//! Filtered subscriptions on top of broadcast receivers.

use crate::error::{EventError, Result};
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::broadcast;

/// Auto-incrementing subscription handle.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SubscriptionId(u64);

static SUBSCRIPTION_COUNTER: AtomicU64 = AtomicU64::new(1);

impl SubscriptionId {
    pub fn generate() -> Self {
        Self(SUBSCRIPTION_COUNTER.fetch_add(1, Ordering::SeqCst))
    }

    pub fn value(&self) -> u64 {
        self.0
    }

    /// Create a subscription ID from a numeric value (for testing)
    #[cfg(test)]
    pub(crate) fn from_value(value: u64) -> Self {
        Self(value)
    }
}

impl fmt::Display for SubscriptionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "sub_{}", self.0)
    }
}

/// Predicate that decides whether an event should be delivered.
pub trait EventFilter<E>: Send + Sync
where
    E: Send + Sync,
{
    /// Return `true` to deliver this event, `false` to skip it.
    fn matches(&self, event: &E) -> bool;
}

/// Closure-based filter.
struct FnFilter<E, F>
where
    E: Send + Sync,
    F: Fn(&E) -> bool + Send + Sync,
{
    predicate: F,
    _phantom: std::marker::PhantomData<E>,
}

impl<E, F> FnFilter<E, F>
where
    E: Send + Sync,
    F: Fn(&E) -> bool + Send + Sync,
{
    fn new(predicate: F) -> Self {
        Self {
            predicate,
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<E, F> EventFilter<E> for FnFilter<E, F>
where
    E: Send + Sync,
    F: Fn(&E) -> bool + Send + Sync,
{
    fn matches(&self, event: &E) -> bool {
        (self.predicate)(event)
    }
}

/// A broadcast receiver with optional filters attached.
/// Events that don't pass every filter are silently skipped.
pub struct Subscription<E>
where
    E: Clone + Send + Sync + 'static,
{
    id: SubscriptionId,
    receiver: broadcast::Receiver<E>,
    filters: Vec<Box<dyn EventFilter<E>>>,
}

impl<E> Subscription<E>
where
    E: Clone + Send + Sync + 'static,
{
    /// Wrap a raw broadcast receiver. No filters by default.
    pub fn new(receiver: broadcast::Receiver<E>) -> Self {
        Self {
            id: SubscriptionId::generate(),
            receiver,
            filters: Vec::new(),
        }
    }

    pub fn id(&self) -> SubscriptionId {
        self.id
    }

    /// Add a filter. Events must pass ALL filters to be delivered.
    pub fn add_filter<F>(&mut self, predicate: F)
    where
        F: Fn(&E) -> bool + Send + Sync + 'static,
    {
        self.filters.push(Box::new(FnFilter::new(predicate)));
    }

    /// Add a pre-boxed filter.
    pub fn add_boxed_filter(&mut self, filter: Box<dyn EventFilter<E>>) {
        self.filters.push(filter);
    }

    /// True if every attached filter accepts the event (or there are no filters).
    fn passes_filters(&self, event: &E) -> bool {
        self.filters.iter().all(|filter| filter.matches(event))
    }

    /// Block until a filtered event arrives, or return `None` if the channel closes.
    pub async fn recv(&mut self) -> Option<E> {
        loop {
            match self.receiver.recv().await {
                Ok(event) => {
                    if self.passes_filters(&event) {
                        return Some(event);
                    }
                    // Event didn't pass filters, continue to next event
                }
                Err(broadcast::error::RecvError::Closed) => {
                    // Channel closed
                    return None;
                }
                Err(broadcast::error::RecvError::Lagged(_)) => {
                    // Some events were missed due to lag, continue receiving
                    continue;
                }
            }
        }
    }

    /// Non-blocking version of `recv`. Returns an error if nothing is ready.
    pub fn try_recv(&mut self) -> Result<E> {
        loop {
            match self.receiver.try_recv() {
                Ok(event) => {
                    if self.passes_filters(&event) {
                        return Ok(event);
                    }
                    // Event didn't pass filters, try next event
                }
                Err(broadcast::error::TryRecvError::Empty) => {
                    return Err(EventError::PublishError {
                        message: "No events available".to_string(),
                    });
                }
                Err(broadcast::error::TryRecvError::Closed) => {
                    return Err(EventError::PublishError {
                        message: "Channel closed".to_string(),
                    });
                }
                Err(broadcast::error::TryRecvError::Lagged(_)) => {
                    // Some events were missed, continue trying
                    continue;
                }
            }
        }
    }

    pub fn filter_count(&self) -> usize {
        self.filters.len()
    }

    pub fn has_filters(&self) -> bool {
        !self.filters.is_empty()
    }
}

impl<E> fmt::Debug for Subscription<E>
where
    E: Clone + Send + Sync + 'static,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Subscription")
            .field("id", &self.id)
            .field("filter_count", &self.filters.len())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bus::channel::TypedChannel;
    use crate::domain::TaskEvent;
    use orcher_common::types::identifiers::{Identifier, TaskId};
    use orcher_common::types::time::Timestamptz;

    #[test]
    fn test_subscription_id_generation() {
        let id1 = SubscriptionId::generate();
        let id2 = SubscriptionId::generate();
        assert_ne!(id1, id2);
        assert!(id2.value() > id1.value());
    }

    #[test]
    fn test_subscription_id_display() {
        let id = SubscriptionId::from_value(42);
        assert_eq!(format!("{}", id), "sub_42");
    }

    #[test]
    fn test_subscription_creation() {
        let channel = TypedChannel::<TaskEvent>::new(1000);
        let receiver = channel.subscribe();
        let subscription = Subscription::new(receiver);

        assert_eq!(subscription.filter_count(), 0);
        assert!(!subscription.has_filters());
    }

    #[test]
    fn test_subscription_add_filter() {
        let channel = TypedChannel::<TaskEvent>::new(1000);
        let receiver = channel.subscribe();
        let mut subscription = Subscription::new(receiver);

        subscription.add_filter(|_| true);
        assert_eq!(subscription.filter_count(), 1);
        assert!(subscription.has_filters());

        subscription.add_filter(|_| true);
        assert_eq!(subscription.filter_count(), 2);
    }

    #[tokio::test]
    async fn test_subscription_recv_no_filter() {
        let channel = TypedChannel::<TaskEvent>::new(1000);
        let receiver = channel.subscribe();
        let mut subscription = Subscription::new(receiver);

        let task_id = TaskId::generate();
        let event = TaskEvent::Submitted {
            task_id,
            submitted_at: Timestamptz::now(),
            priority: 5,
        };

        channel.publish(event.clone()).await.unwrap();

        let received = subscription.recv().await.unwrap();
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
    async fn test_subscription_with_filter() {
        let channel = TypedChannel::<TaskEvent>::new(1000);
        let receiver = channel.subscribe();
        let mut subscription = Subscription::new(receiver);

        // Only accept high-priority tasks (priority >= 8)
        subscription.add_filter(
            |event| matches!(event, TaskEvent::Submitted { priority, .. } if *priority >= 8),
        );

        // Publish low-priority event (should be filtered out)
        let task_id1 = TaskId::generate();
        let event1 = TaskEvent::Submitted {
            task_id: task_id1,
            submitted_at: Timestamptz::now(),
            priority: 5,
        };
        channel.publish(event1).await.unwrap();

        // Publish high-priority event (should be received)
        let task_id2 = TaskId::generate();
        let event2 = TaskEvent::Submitted {
            task_id: task_id2,
            submitted_at: Timestamptz::now(),
            priority: 9,
        };
        channel.publish(event2).await.unwrap();

        // Should receive only the high-priority event
        let received = subscription.recv().await.unwrap();
        match received {
            TaskEvent::Submitted {
                task_id, priority, ..
            } => {
                assert_eq!(task_id, task_id2);
                assert_eq!(priority, 9);
            }
            _ => panic!("Expected TaskEvent::Submitted"),
        }
    }

    #[tokio::test]
    async fn test_subscription_multiple_filters() {
        let channel = TypedChannel::<TaskEvent>::new(1000);
        let receiver = channel.subscribe();
        let mut subscription = Subscription::new(receiver);

        let specific_task_id = TaskId::generate();

        // Filter 1: Only high priority
        subscription.add_filter(
            |event| matches!(event, TaskEvent::Submitted { priority, .. } if *priority >= 8),
        );

        // Filter 2: Only specific task ID
        subscription.add_filter(move |event| {
            matches!(event, TaskEvent::Submitted { task_id, .. } if *task_id == specific_task_id)
        });

        // Publish event matching filter 1 but not filter 2
        let task_id1 = TaskId::generate();
        let event1 = TaskEvent::Submitted {
            task_id: task_id1,
            submitted_at: Timestamptz::now(),
            priority: 9,
        };
        channel.publish(event1).await.unwrap();

        // Publish event matching both filters
        let event2 = TaskEvent::Submitted {
            task_id: specific_task_id,
            submitted_at: Timestamptz::now(),
            priority: 10,
        };
        channel.publish(event2).await.unwrap();

        // Should receive only the event matching both filters
        let received = subscription.recv().await.unwrap();
        match received {
            TaskEvent::Submitted {
                task_id, priority, ..
            } => {
                assert_eq!(task_id, specific_task_id);
                assert_eq!(priority, 10);
            }
            _ => panic!("Expected TaskEvent::Submitted"),
        }
    }

    #[test]
    fn test_subscription_try_recv_empty() {
        let channel = TypedChannel::<TaskEvent>::new(1000);
        let receiver = channel.subscribe();
        let mut subscription = Subscription::new(receiver);

        let result = subscription.try_recv();
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_subscription_try_recv_with_event() {
        let channel = TypedChannel::<TaskEvent>::new(1000);
        let receiver = channel.subscribe();
        let mut subscription = Subscription::new(receiver);

        let task_id = TaskId::generate();
        let event = TaskEvent::Submitted {
            task_id,
            submitted_at: Timestamptz::now(),
            priority: 5,
        };

        channel.publish(event).await.unwrap();

        // Give a small delay for the event to propagate
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        let result = subscription.try_recv();
        assert!(result.is_ok());

        let received = result.unwrap();
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

    #[test]
    fn test_subscription_debug() {
        let channel = TypedChannel::<TaskEvent>::new(1000);
        let receiver = channel.subscribe();
        let subscription = Subscription::new(receiver);

        let debug_str = format!("{:?}", subscription);
        assert!(debug_str.contains("Subscription"));
        assert!(debug_str.contains("filter_count"));
    }
}
