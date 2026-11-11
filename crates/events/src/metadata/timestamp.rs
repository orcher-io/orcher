//! Tracks when an event occurred, was processed, and was published.

use jiff::SignedDuration;
use orcher_common::types::time::Timestamptz;
use serde::{Deserialize, Serialize};

/// Lifecycle timestamps for an event: occurred -> processed -> published.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EventTimestamp {
    pub occurred_at: Timestamptz,
    pub processed_at: Option<Timestamptz>,
    pub published_at: Option<Timestamptz>,
}

impl EventTimestamp {
    pub fn now() -> Self {
        Self {
            occurred_at: Timestamptz::now(),
            processed_at: None,
            published_at: None,
        }
    }

    pub fn with_time(occurred_at: Timestamptz) -> Self {
        Self {
            occurred_at,
            processed_at: None,
            published_at: None,
        }
    }

    pub fn mark_processed(&mut self) {
        self.processed_at = Some(Timestamptz::now());
    }

    pub fn mark_processed_at(&mut self, time: Timestamptz) {
        self.processed_at = Some(time);
    }

    pub fn mark_published(&mut self) {
        self.published_at = Some(Timestamptz::now());
    }

    pub fn mark_published_at(&mut self, time: Timestamptz) {
        self.published_at = Some(time);
    }

    /// `processed_at - occurred_at`, or None if not yet processed.
    pub fn processing_duration(&self) -> Option<SignedDuration> {
        self.processed_at
            .map(|processed| processed - self.occurred_at)
    }

    /// `published_at - processed_at`, or None if either is missing.
    pub fn publishing_duration(&self) -> Option<SignedDuration> {
        match (self.processed_at, self.published_at) {
            (Some(processed), Some(published)) => Some(published - processed),
            _ => None,
        }
    }

    /// `published_at - occurred_at`, or None if not yet published.
    pub fn total_duration(&self) -> Option<SignedDuration> {
        self.published_at
            .map(|published| published - self.occurred_at)
    }

    /// How long ago this event occurred.
    pub fn age(&self) -> SignedDuration {
        Timestamptz::now() - self.occurred_at
    }

    pub fn is_processed(&self) -> bool {
        self.processed_at.is_some()
    }

    pub fn is_published(&self) -> bool {
        self.published_at.is_some()
    }

    pub fn processing_latency_ms(&self) -> Option<i64> {
        self.processing_duration().map(|d| d.as_millis() as i64)
    }

    pub fn publishing_latency_ms(&self) -> Option<i64> {
        self.publishing_duration().map(|d| d.as_millis() as i64)
    }

    pub fn total_latency_ms(&self) -> Option<i64> {
        self.total_duration().map(|d| d.as_millis() as i64)
    }
}

impl Default for EventTimestamp {
    fn default() -> Self {
        Self::now()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::Duration as StdDuration;

    #[test]
    fn test_timestamp_creation() {
        let timestamp = EventTimestamp::now();
        assert!(timestamp.processed_at.is_none());
        assert!(timestamp.published_at.is_none());
    }

    #[test]
    fn test_timestamp_with_time() {
        let time = Timestamptz::now();
        let timestamp = EventTimestamp::with_time(time);
        assert_eq!(timestamp.occurred_at, time);
    }

    #[test]
    fn test_mark_processed() {
        let mut timestamp = EventTimestamp::now();
        assert!(!timestamp.is_processed());

        timestamp.mark_processed();
        assert!(timestamp.is_processed());
        assert!(timestamp.processed_at.is_some());
    }

    #[test]
    fn test_mark_processed_at() {
        let mut timestamp = EventTimestamp::now();
        let process_time = Timestamptz::now();

        timestamp.mark_processed_at(process_time);
        assert_eq!(timestamp.processed_at, Some(process_time));
    }

    #[test]
    fn test_mark_published() {
        let mut timestamp = EventTimestamp::now();
        assert!(!timestamp.is_published());

        timestamp.mark_published();
        assert!(timestamp.is_published());
        assert!(timestamp.published_at.is_some());
    }

    #[test]
    fn test_mark_published_at() {
        let mut timestamp = EventTimestamp::now();
        let publish_time = Timestamptz::now();

        timestamp.mark_published_at(publish_time);
        assert_eq!(timestamp.published_at, Some(publish_time));
    }

    #[test]
    fn test_processing_duration() {
        let mut timestamp = EventTimestamp::now();
        thread::sleep(StdDuration::from_millis(10));
        timestamp.mark_processed();

        let duration = timestamp.processing_duration();
        assert!(duration.is_some());
        assert!(duration.unwrap().as_millis() as i64 >= 10);
    }

    #[test]
    fn test_processing_duration_not_processed() {
        let timestamp = EventTimestamp::now();
        assert!(timestamp.processing_duration().is_none());
    }

    #[test]
    fn test_publishing_duration() {
        let mut timestamp = EventTimestamp::now();
        timestamp.mark_processed();
        thread::sleep(StdDuration::from_millis(10));
        timestamp.mark_published();

        let duration = timestamp.publishing_duration();
        assert!(duration.is_some());
        assert!(duration.unwrap().as_millis() as i64 >= 10);
    }

    #[test]
    fn test_publishing_duration_not_published() {
        let mut timestamp = EventTimestamp::now();
        timestamp.mark_processed();
        assert!(timestamp.publishing_duration().is_none());
    }

    #[test]
    fn test_total_duration() {
        let mut timestamp = EventTimestamp::now();
        thread::sleep(StdDuration::from_millis(10));
        timestamp.mark_processed();
        thread::sleep(StdDuration::from_millis(10));
        timestamp.mark_published();

        let duration = timestamp.total_duration();
        assert!(duration.is_some());
        assert!(duration.unwrap().as_millis() as i64 >= 20);
    }

    #[test]
    fn test_total_duration_not_published() {
        let mut timestamp = EventTimestamp::now();
        timestamp.mark_processed();
        assert!(timestamp.total_duration().is_none());
    }

    #[test]
    fn test_age() {
        let timestamp = EventTimestamp::now();
        thread::sleep(StdDuration::from_millis(10));

        let age = timestamp.age();
        assert!(age.as_millis() as i64 >= 10);
    }

    #[test]
    fn test_processing_latency_ms() {
        let mut timestamp = EventTimestamp::now();
        thread::sleep(StdDuration::from_millis(10));
        timestamp.mark_processed();

        let latency = timestamp.processing_latency_ms();
        assert!(latency.is_some());
        assert!(latency.unwrap() >= 10);
    }

    #[test]
    fn test_publishing_latency_ms() {
        let mut timestamp = EventTimestamp::now();
        timestamp.mark_processed();
        thread::sleep(StdDuration::from_millis(10));
        timestamp.mark_published();

        let latency = timestamp.publishing_latency_ms();
        assert!(latency.is_some());
        assert!(latency.unwrap() >= 10);
    }

    #[test]
    fn test_total_latency_ms() {
        let mut timestamp = EventTimestamp::now();
        thread::sleep(StdDuration::from_millis(10));
        timestamp.mark_processed();
        thread::sleep(StdDuration::from_millis(10));
        timestamp.mark_published();

        let latency = timestamp.total_latency_ms();
        assert!(latency.is_some());
        assert!(latency.unwrap() >= 20);
    }

    #[test]
    fn test_is_processed() {
        let mut timestamp = EventTimestamp::now();
        assert!(!timestamp.is_processed());

        timestamp.mark_processed();
        assert!(timestamp.is_processed());
    }

    #[test]
    fn test_is_published() {
        let mut timestamp = EventTimestamp::now();
        assert!(!timestamp.is_published());

        timestamp.mark_published();
        assert!(timestamp.is_published());
    }

    #[test]
    fn test_complete_lifecycle() {
        let mut timestamp = EventTimestamp::now();

        // Initial state
        assert!(!timestamp.is_processed());
        assert!(!timestamp.is_published());

        // Mark processed
        thread::sleep(StdDuration::from_millis(5));
        timestamp.mark_processed();
        assert!(timestamp.is_processed());
        assert!(!timestamp.is_published());

        // Mark published
        thread::sleep(StdDuration::from_millis(5));
        timestamp.mark_published();
        assert!(timestamp.is_processed());
        assert!(timestamp.is_published());

        // All durations should be available
        assert!(timestamp.processing_duration().is_some());
        assert!(timestamp.publishing_duration().is_some());
        assert!(timestamp.total_duration().is_some());
    }

    #[test]
    fn test_timestamp_serialization() {
        let mut timestamp = EventTimestamp::now();
        timestamp.mark_processed();
        timestamp.mark_published();

        let json = serde_json::to_string(&timestamp).unwrap();
        let deserialized: EventTimestamp = serde_json::from_str(&json).unwrap();

        assert_eq!(timestamp.occurred_at, deserialized.occurred_at);
        assert_eq!(timestamp.processed_at, deserialized.processed_at);
        assert_eq!(timestamp.published_at, deserialized.published_at);
    }

    #[test]
    fn test_timestamp_default() {
        let timestamp = EventTimestamp::default();
        assert!(!timestamp.is_processed());
        assert!(!timestamp.is_published());
    }
}
