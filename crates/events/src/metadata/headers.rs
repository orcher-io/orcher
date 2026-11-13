//! Custom key-value headers attached to events.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;

/// A header value — string, number, bool, or ISO 8601 timestamp.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum HeaderValue {
    String(String),
    Integer(i64),
    Float(f64),
    Boolean(bool),
    /// ISO 8601 timestamp string.
    Timestamp(String),
}

impl HeaderValue {
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Self::String(s) => Some(s),
            _ => None,
        }
    }

    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Self::Integer(i) => Some(*i),
            _ => None,
        }
    }

    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Self::Float(f) => Some(*f),
            _ => None,
        }
    }

    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Self::Boolean(b) => Some(*b),
            _ => None,
        }
    }

    pub fn is_string(&self) -> bool {
        matches!(self, Self::String(_))
    }

    pub fn is_integer(&self) -> bool {
        matches!(self, Self::Integer(_))
    }

    pub fn is_float(&self) -> bool {
        matches!(self, Self::Float(_))
    }

    pub fn is_boolean(&self) -> bool {
        matches!(self, Self::Boolean(_))
    }

    pub fn is_timestamp(&self) -> bool {
        matches!(self, Self::Timestamp(_))
    }
}

impl fmt::Display for HeaderValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::String(s) => write!(f, "{}", s),
            Self::Integer(i) => write!(f, "{}", i),
            Self::Float(fl) => write!(f, "{}", fl),
            Self::Boolean(b) => write!(f, "{}", b),
            Self::Timestamp(t) => write!(f, "{}", t),
        }
    }
}

impl From<String> for HeaderValue {
    fn from(s: String) -> Self {
        Self::String(s)
    }
}

impl From<&str> for HeaderValue {
    fn from(s: &str) -> Self {
        Self::String(s.to_string())
    }
}

impl From<i64> for HeaderValue {
    fn from(i: i64) -> Self {
        Self::Integer(i)
    }
}

impl From<i32> for HeaderValue {
    fn from(i: i32) -> Self {
        Self::Integer(i as i64)
    }
}

impl From<f64> for HeaderValue {
    fn from(f: f64) -> Self {
        Self::Float(f)
    }
}

impl From<bool> for HeaderValue {
    fn from(b: bool) -> Self {
        Self::Boolean(b)
    }
}

/// String-keyed bag of typed values attached to an event.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EventHeaders {
    headers: HashMap<String, HeaderValue>,
}

impl EventHeaders {
    pub fn new() -> Self {
        Self {
            headers: HashMap::new(),
        }
    }

    pub fn set(&mut self, key: impl Into<String>, value: impl Into<HeaderValue>) {
        self.headers.insert(key.into(), value.into());
    }

    pub fn get(&self, key: &str) -> Option<&HeaderValue> {
        self.headers.get(key)
    }

    pub fn remove(&mut self, key: &str) -> Option<HeaderValue> {
        self.headers.remove(key)
    }

    pub fn contains(&self, key: &str) -> bool {
        self.headers.contains_key(key)
    }

    pub fn len(&self) -> usize {
        self.headers.len()
    }

    pub fn is_empty(&self) -> bool {
        self.headers.is_empty()
    }

    pub fn clear(&mut self) {
        self.headers.clear();
    }

    pub fn iter(&self) -> impl Iterator<Item = (&String, &HeaderValue)> {
        self.headers.iter()
    }

    pub fn keys(&self) -> impl Iterator<Item = &String> {
        self.headers.keys()
    }

    pub fn values(&self) -> impl Iterator<Item = &HeaderValue> {
        self.headers.values()
    }

    /// Merge from `other`. Duplicate keys are overwritten.
    pub fn merge(&mut self, other: EventHeaders) {
        self.headers.extend(other.headers);
    }
}

impl Default for EventHeaders {
    fn default() -> Self {
        Self::new()
    }
}

impl From<HashMap<String, HeaderValue>> for EventHeaders {
    fn from(headers: HashMap<String, HeaderValue>) -> Self {
        Self { headers }
    }
}

impl From<EventHeaders> for HashMap<String, HeaderValue> {
    fn from(headers: EventHeaders) -> Self {
        headers.headers
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_header_value_string() {
        let value = HeaderValue::String("test".to_string());
        assert!(value.is_string());
        assert_eq!(value.as_str(), Some("test"));
        assert_eq!(value.to_string(), "test");
    }

    #[test]
    fn test_header_value_integer() {
        let value = HeaderValue::Integer(42);
        assert!(value.is_integer());
        assert_eq!(value.as_i64(), Some(42));
        assert_eq!(value.to_string(), "42");
    }

    #[test]
    fn test_header_value_float() {
        let value = HeaderValue::Float(2.5);
        assert!(value.is_float());
        assert_eq!(value.as_f64(), Some(2.5));
    }

    #[test]
    fn test_header_value_boolean() {
        let value = HeaderValue::Boolean(true);
        assert!(value.is_boolean());
        assert_eq!(value.as_bool(), Some(true));
        assert_eq!(value.to_string(), "true");
    }

    #[test]
    fn test_header_value_from_string() {
        let value: HeaderValue = "test".into();
        assert_eq!(value.as_str(), Some("test"));
    }

    #[test]
    fn test_header_value_from_i32() {
        let value: HeaderValue = 42i32.into();
        assert_eq!(value.as_i64(), Some(42));
    }

    #[test]
    fn test_header_value_from_i64() {
        let value: HeaderValue = 42i64.into();
        assert_eq!(value.as_i64(), Some(42));
    }

    #[test]
    fn test_header_value_from_f64() {
        let value: HeaderValue = 2.5f64.into();
        assert_eq!(value.as_f64(), Some(2.5));
    }

    #[test]
    fn test_header_value_from_bool() {
        let value: HeaderValue = true.into();
        assert_eq!(value.as_bool(), Some(true));
    }

    #[test]
    fn test_event_headers_creation() {
        let headers = EventHeaders::new();
        assert!(headers.is_empty());
        assert_eq!(headers.len(), 0);
    }

    #[test]
    fn test_event_headers_set_get() {
        let mut headers = EventHeaders::new();
        headers.set("key", "value");

        assert_eq!(headers.get("key").and_then(|v| v.as_str()), Some("value"));
        assert!(headers.contains("key"));
        assert_eq!(headers.len(), 1);
    }

    #[test]
    fn test_event_headers_multiple_types() {
        let mut headers = EventHeaders::new();
        headers.set("string", "value");
        headers.set("integer", 42);
        headers.set("float", 2.5);
        headers.set("boolean", true);

        assert_eq!(
            headers.get("string").and_then(|v| v.as_str()),
            Some("value")
        );
        assert_eq!(headers.get("integer").and_then(|v| v.as_i64()), Some(42));
        assert_eq!(headers.get("float").and_then(|v| v.as_f64()), Some(2.5));
        assert_eq!(headers.get("boolean").and_then(|v| v.as_bool()), Some(true));
    }

    #[test]
    fn test_event_headers_remove() {
        let mut headers = EventHeaders::new();
        headers.set("key", "value");

        let removed = headers.remove("key");
        assert!(removed.is_some());
        assert!(!headers.contains("key"));
        assert!(headers.is_empty());
    }

    #[test]
    fn test_event_headers_clear() {
        let mut headers = EventHeaders::new();
        headers.set("key1", "value1");
        headers.set("key2", "value2");

        headers.clear();
        assert!(headers.is_empty());
    }

    #[test]
    fn test_event_headers_iter() {
        let mut headers = EventHeaders::new();
        headers.set("key1", "value1");
        headers.set("key2", "value2");

        let count = headers.iter().count();
        assert_eq!(count, 2);
    }

    #[test]
    fn test_event_headers_keys() {
        let mut headers = EventHeaders::new();
        headers.set("key1", "value1");
        headers.set("key2", "value2");

        let keys: Vec<&String> = headers.keys().collect();
        assert_eq!(keys.len(), 2);
        assert!(keys.contains(&&"key1".to_string()));
        assert!(keys.contains(&&"key2".to_string()));
    }

    #[test]
    fn test_event_headers_merge() {
        let mut headers1 = EventHeaders::new();
        headers1.set("key1", "value1");

        let mut headers2 = EventHeaders::new();
        headers2.set("key2", "value2");

        headers1.merge(headers2);
        assert_eq!(headers1.len(), 2);
        assert!(headers1.contains("key1"));
        assert!(headers1.contains("key2"));
    }

    #[test]
    fn test_event_headers_merge_overwrite() {
        let mut headers1 = EventHeaders::new();
        headers1.set("key", "value1");

        let mut headers2 = EventHeaders::new();
        headers2.set("key", "value2");

        headers1.merge(headers2);
        assert_eq!(headers1.len(), 1);
        assert_eq!(headers1.get("key").and_then(|v| v.as_str()), Some("value2"));
    }

    #[test]
    fn test_event_headers_serialization() {
        let mut headers = EventHeaders::new();
        headers.set("key", "value");
        headers.set("count", 42);

        let json = serde_json::to_string(&headers).unwrap();
        let deserialized: EventHeaders = serde_json::from_str(&json).unwrap();

        assert_eq!(headers, deserialized);
    }
}
