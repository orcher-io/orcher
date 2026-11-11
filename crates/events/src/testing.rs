//! Test helpers for event-based code.

#[cfg(feature = "replay")]
pub use in_memory::InMemoryEventStore;

#[cfg(feature = "replay")]
mod in_memory {
    use crate::error::{EventError, Result};
    use crate::metadata::StreamId;
    use crate::replay::{
        AppendResult, EventQuery, EventStore, QueryResult, ReadDirection, StoredEvent,
        StreamPosition,
    };
    use crate::Event;
    use async_trait::async_trait;
    use orcher_common::types::time::Timestamptz;
    use std::collections::HashMap;
    use std::sync::Arc;
    use tokio::sync::RwLock;

    /// Fake event store backed by a `HashMap`. Good for unit tests, not for prod.
    #[derive(Clone)]
    pub struct InMemoryEventStore {
        streams: Arc<RwLock<HashMap<StreamId, Vec<StoredEvent>>>>,
        next_global_position: Arc<RwLock<u64>>,
        next_event_id: Arc<RwLock<u64>>,
    }

    impl InMemoryEventStore {
        /// Create an empty store.
        pub fn new() -> Self {
            Self {
                streams: Arc::new(RwLock::new(HashMap::new())),
                next_global_position: Arc::new(RwLock::new(1)),
                next_event_id: Arc::new(RwLock::new(1)),
            }
        }

        /// Dump all events for a given stream.
        pub async fn get_all_events(&self, stream_id: &StreamId) -> Vec<StoredEvent> {
            let streams = self.streams.read().await;
            streams.get(stream_id).cloned().unwrap_or_default()
        }

        /// Total events across all streams.
        pub async fn total_event_count(&self) -> usize {
            let streams = self.streams.read().await;
            streams.values().map(|v| v.len()).sum()
        }

        /// How many distinct streams exist.
        pub async fn stream_count(&self) -> usize {
            let streams = self.streams.read().await;
            streams.len()
        }

        /// Wipe everything — reset to empty state.
        pub async fn clear(&self) {
            let mut streams = self.streams.write().await;
            streams.clear();
            let mut next_global = self.next_global_position.write().await;
            *next_global = 1;
            let mut next_id = self.next_event_id.write().await;
            *next_id = 1;
        }

        /// Find all events matching a given event type string.
        pub async fn get_events_by_type(&self, event_type: &str) -> Vec<StoredEvent> {
            let streams = self.streams.read().await;
            streams
                .values()
                .flat_map(|v| v.iter())
                .filter(|e| e.event_type == event_type)
                .cloned()
                .collect()
        }
    }

    impl Default for InMemoryEventStore {
        fn default() -> Self {
            Self::new()
        }
    }

    #[async_trait]
    impl EventStore for InMemoryEventStore {
        async fn append_to_stream(
            &self,
            stream_id: &StreamId,
            expected_position: StreamPosition,
            events: Vec<Event>,
        ) -> Result<AppendResult> {
            let mut streams = self.streams.write().await;
            let mut next_global = self.next_global_position.write().await;
            let mut next_id = self.next_event_id.write().await;

            // Get or create stream
            let stream = streams.entry(stream_id.clone()).or_insert_with(Vec::new);

            // Check expected position (optimistic concurrency)
            let current_position = stream.len() as u64;
            match expected_position {
                StreamPosition::Start => {
                    if !stream.is_empty() {
                        return Err(EventError::concurrency_conflict(0, current_position));
                    }
                }
                StreamPosition::Position(pos) => {
                    if current_position != pos {
                        return Err(EventError::concurrency_conflict(pos, current_position));
                    }
                }
                StreamPosition::End => {
                    // No check - append at end
                }
            }

            let first_global_position = *next_global;
            let mut events_appended = 0;

            // Append events
            for event in events {
                let stored_event = StoredEvent {
                    event_id: *next_id,
                    stream_id: stream_id.clone(),
                    event_type: event.event_type.clone(),
                    version: 1, // Default version for simple events
                    stream_position: stream.len() as u64 + 1, // 1-based
                    global_position: *next_global,
                    data: event.data.clone(),
                    metadata: serde_json::to_value(&event.metadata).unwrap_or_default(),
                    timestamp: event.timestamp,
                    stored_at: Timestamptz::now(),
                    correlation_id: event.correlation_id().map(|s| s.to_string()),
                    causation_id: event.causation_id().map(|s| s.to_string()),
                };

                stream.push(stored_event);
                *next_id += 1;
                *next_global += 1;
                events_appended += 1;
            }

            Ok(AppendResult {
                stream_position: stream.len() as u64,
                global_position: first_global_position,
                events_appended,
            })
        }

        async fn read_stream(
            &self,
            stream_id: &StreamId,
            from_position: StreamPosition,
            max_count: usize,
        ) -> Result<Vec<StoredEvent>> {
            let streams = self.streams.read().await;
            let stream = streams.get(stream_id);

            if stream.is_none() {
                return Ok(Vec::new());
            }

            let stream = stream.unwrap();

            let start_idx = match from_position {
                StreamPosition::Start => 0,
                StreamPosition::Position(pos) => {
                    if pos == 0 {
                        0
                    } else {
                        (pos - 1) as usize // Convert 1-based to 0-based
                    }
                }
                StreamPosition::End => stream.len(),
            };

            Ok(stream
                .iter()
                .skip(start_idx)
                .take(max_count)
                .cloned()
                .collect())
        }

        async fn read_stream_backwards(
            &self,
            stream_id: &StreamId,
            from_position: Option<StreamPosition>,
            max_count: usize,
        ) -> Result<Vec<StoredEvent>> {
            let streams = self.streams.read().await;
            let stream = streams.get(stream_id);

            if stream.is_none() {
                return Ok(Vec::new());
            }

            let stream = stream.unwrap();

            let end_idx = match from_position {
                None | Some(StreamPosition::End) => stream.len(),
                Some(StreamPosition::Start) => 0,
                Some(StreamPosition::Position(pos)) => pos as usize,
            };

            let start_idx = end_idx.saturating_sub(max_count);

            let mut events: Vec<StoredEvent> = stream
                .iter()
                .skip(start_idx)
                .take(end_idx - start_idx)
                .cloned()
                .collect();

            events.reverse();
            Ok(events)
        }

        async fn get_stream_position(&self, stream_id: &StreamId) -> Result<u64> {
            let streams = self.streams.read().await;
            match streams.get(stream_id) {
                Some(stream) => Ok(stream.len() as u64),
                None => Ok(0),
            }
        }

        async fn stream_exists(&self, stream_id: &StreamId) -> Result<bool> {
            let streams = self.streams.read().await;
            Ok(streams.contains_key(stream_id))
        }

        async fn query_events(&self, query: &EventQuery) -> Result<QueryResult> {
            let streams = self.streams.read().await;

            let mut events: Vec<StoredEvent> = streams
                .iter()
                .filter(|(stream_id, _)| {
                    // Filter by stream_id
                    if let Some(ref target_stream_id) = query.stream_id {
                        if *stream_id != target_stream_id {
                            return false;
                        }
                    }

                    // Filter by category
                    if let Some(ref category) = query.category {
                        if stream_id.category() != category {
                            return false;
                        }
                    }

                    true
                })
                .flat_map(|(_, events)| events.iter())
                .filter(|e| {
                    // Filter by event types
                    if !query.event_types.is_empty() && !query.event_types.contains(&e.event_type) {
                        return false;
                    }

                    // Filter by position range
                    if let Some(from_pos) = query.from_position {
                        if e.global_position < from_pos {
                            return false;
                        }
                    }
                    if let Some(to_pos) = query.to_position {
                        if e.global_position >= to_pos {
                            return false;
                        }
                    }

                    // Filter by timestamp range
                    if let Some(from_ts) = query.from_timestamp {
                        if e.timestamp < from_ts {
                            return false;
                        }
                    }
                    if let Some(to_ts) = query.to_timestamp {
                        if e.timestamp >= to_ts {
                            return false;
                        }
                    }

                    true
                })
                .cloned()
                .collect();

            // Sort by global position
            if query.direction == ReadDirection::Forward {
                events.sort_by_key(|e| e.global_position);
            } else {
                events.sort_by_key(|e| std::cmp::Reverse(e.global_position));
            }

            // Apply limit and check if there are more
            let has_more = if let Some(limit) = query.limit {
                let has_more = events.len() > limit;
                events.truncate(limit);
                has_more
            } else {
                false
            };

            Ok(QueryResult { events, has_more })
        }

        async fn read_all(&self, from_position: u64, max_count: usize) -> Result<Vec<StoredEvent>> {
            let streams = self.streams.read().await;

            let mut all_events: Vec<StoredEvent> = streams
                .values()
                .flat_map(|events| events.iter())
                .filter(|e| e.global_position >= from_position)
                .cloned()
                .collect();

            all_events.sort_by_key(|e| e.global_position);
            all_events.truncate(max_count);

            Ok(all_events)
        }

        async fn get_global_position(&self) -> Result<u64> {
            let next_global = self.next_global_position.read().await;
            Ok(*next_global - 1) // Return the last assigned position
        }

        async fn delete_stream(&self, stream_id: &StreamId) -> Result<()> {
            let mut streams = self.streams.write().await;
            streams.remove(stream_id);
            Ok(())
        }

        async fn get_event_by_id(&self, event_id: u64) -> Result<Option<StoredEvent>> {
            let streams = self.streams.read().await;

            for events in streams.values() {
                if let Some(event) = events.iter().find(|e| e.event_id == event_id) {
                    return Ok(Some(event.clone()));
                }
            }

            Ok(None)
        }

        async fn query_category(
            &self,
            category: &str,
            from_position: u64,
            max_count: usize,
        ) -> Result<Vec<StoredEvent>> {
            let streams = self.streams.read().await;

            let mut events: Vec<StoredEvent> = streams
                .iter()
                .filter(|(stream_id, _)| stream_id.category() == category)
                .flat_map(|(_, events)| events.iter())
                .filter(|e| e.global_position >= from_position)
                .cloned()
                .collect();

            events.sort_by_key(|e| e.global_position);
            events.truncate(max_count);

            Ok(events)
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use crate::metadata::StreamId;

        #[tokio::test]
        async fn test_append_and_read() {
            let store = InMemoryEventStore::new();
            let stream_id = StreamId::new("test", "1");

            let events = vec![
                Event::new("TestEvent1".to_string(), serde_json::json!({"value": 1})),
                Event::new("TestEvent2".to_string(), serde_json::json!({"value": 2})),
            ];

            let result = store
                .append_to_stream(&stream_id, StreamPosition::End, events)
                .await
                .unwrap();

            assert_eq!(result.stream_position, 2);
            assert_eq!(result.events_appended, 2);

            let stored = store
                .read_stream(&stream_id, StreamPosition::Start, 100)
                .await
                .unwrap();

            assert_eq!(stored.len(), 2);
            assert_eq!(stored[0].stream_position, 1);
            assert_eq!(stored[1].stream_position, 2);
        }

        #[tokio::test]
        async fn test_concurrency_check() {
            let store = InMemoryEventStore::new();
            let stream_id = StreamId::new("test", "1");

            // Append first event
            let events = vec![Event::new(
                "TestEvent1".to_string(),
                serde_json::json!({"value": 1}),
            )];
            store
                .append_to_stream(&stream_id, StreamPosition::Start, events)
                .await
                .unwrap();

            // Try to append at start again - should fail
            let events = vec![Event::new(
                "TestEvent2".to_string(),
                serde_json::json!({"value": 2}),
            )];
            let result = store
                .append_to_stream(&stream_id, StreamPosition::Start, events)
                .await;

            assert!(result.is_err());
        }

        #[tokio::test]
        async fn test_read_backwards() {
            let store = InMemoryEventStore::new();
            let stream_id = StreamId::new("test", "1");

            let events = vec![
                Event::new("Event1".to_string(), serde_json::json!({"value": 1})),
                Event::new("Event2".to_string(), serde_json::json!({"value": 2})),
                Event::new("Event3".to_string(), serde_json::json!({"value": 3})),
            ];

            store
                .append_to_stream(&stream_id, StreamPosition::End, events)
                .await
                .unwrap();

            let stored = store
                .read_stream_backwards(&stream_id, None, 2)
                .await
                .unwrap();

            assert_eq!(stored.len(), 2);
            assert_eq!(stored[0].event_type, "Event3");
            assert_eq!(stored[1].event_type, "Event2");
        }

        #[tokio::test]
        async fn test_query_events() {
            let store = InMemoryEventStore::new();
            let stream1 = StreamId::new("orders", "1");
            let stream2 = StreamId::new("orders", "2");

            let events1 = vec![
                Event::new(
                    "OrderCreated".to_string(),
                    serde_json::json!({"order_id": 1}),
                ),
                Event::new("OrderPaid".to_string(), serde_json::json!({"order_id": 1})),
            ];

            let events2 = vec![Event::new(
                "OrderCreated".to_string(),
                serde_json::json!({"order_id": 2}),
            )];

            store
                .append_to_stream(&stream1, StreamPosition::End, events1)
                .await
                .unwrap();
            store
                .append_to_stream(&stream2, StreamPosition::End, events2)
                .await
                .unwrap();

            // Query by event type
            let query =
                crate::replay::EventQuery::new().with_event_types(vec!["OrderCreated".to_string()]);

            let result = store.query_events(&query).await.unwrap();
            assert_eq!(result.events.len(), 2);

            // Query by category
            let query = crate::replay::EventQuery::new().for_category("orders".to_string());

            let result = store.query_events(&query).await.unwrap();
            assert_eq!(result.events.len(), 3);
        }

        #[tokio::test]
        async fn test_clear() {
            let store = InMemoryEventStore::new();
            let stream_id = StreamId::new("test", "1");

            let events = vec![Event::new(
                "TestEvent".to_string(),
                serde_json::json!({"value": 1}),
            )];
            store
                .append_to_stream(&stream_id, StreamPosition::End, events)
                .await
                .unwrap();

            assert_eq!(store.total_event_count().await, 1);

            store.clear().await;

            assert_eq!(store.total_event_count().await, 0);
            assert_eq!(store.stream_count().await, 0);
        }

        #[tokio::test]
        async fn test_stream_exists() {
            let store = InMemoryEventStore::new();
            let stream_id = StreamId::new("test", "1");

            assert!(!store.stream_exists(&stream_id).await.unwrap());

            let events = vec![Event::new(
                "TestEvent".to_string(),
                serde_json::json!({"value": 1}),
            )];
            store
                .append_to_stream(&stream_id, StreamPosition::End, events)
                .await
                .unwrap();

            assert!(store.stream_exists(&stream_id).await.unwrap());
        }

        #[tokio::test]
        async fn test_get_event_by_id() {
            let store = InMemoryEventStore::new();
            let stream_id = StreamId::new("test", "1");

            let events = vec![Event::new(
                "TestEvent".to_string(),
                serde_json::json!({"value": 1}),
            )];
            store
                .append_to_stream(&stream_id, StreamPosition::End, events)
                .await
                .unwrap();

            let event = store.get_event_by_id(1).await.unwrap();
            assert!(event.is_some());
            assert_eq!(event.unwrap().event_id, 1);

            let event = store.get_event_by_id(999).await.unwrap();
            assert!(event.is_none());
        }

        #[tokio::test]
        async fn test_query_category() {
            let store = InMemoryEventStore::new();
            let stream1 = StreamId::new("orders", "1");
            let stream2 = StreamId::new("tasks", "1");

            let events1 = vec![Event::new(
                "OrderCreated".to_string(),
                serde_json::json!({"order_id": 1}),
            )];
            let events2 = vec![Event::new(
                "TaskCreated".to_string(),
                serde_json::json!({"task_id": 1}),
            )];

            store
                .append_to_stream(&stream1, StreamPosition::End, events1)
                .await
                .unwrap();
            store
                .append_to_stream(&stream2, StreamPosition::End, events2)
                .await
                .unwrap();

            let events = store.query_category("orders", 0, 100).await.unwrap();
            assert_eq!(events.len(), 1);
            assert_eq!(events[0].event_type, "OrderCreated");

            let events = store.query_category("tasks", 0, 100).await.unwrap();
            assert_eq!(events.len(), 1);
            assert_eq!(events[0].event_type, "TaskCreated");
        }
    }
}
