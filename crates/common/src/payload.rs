//! Rust-native helper types for working with Payload messages.

use crate::error::OrcherError;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// A piece of data with encoding metadata, compatible with the proto `Payload` definition.
///
/// Standard metadata keys: `"encoding"` (e.g. "json", "binary/protobuf", "json/gzip")
/// and `"content-type"` (e.g. "application/json").
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Payload {
    pub data: Vec<u8>,
    pub metadata: HashMap<String, Vec<u8>>,
}

impl Payload {
    pub fn new(data: Vec<u8>, metadata: HashMap<String, Vec<u8>>) -> Self {
        Self { data, metadata }
    }

    pub fn empty() -> Self {
        Self {
            data: Vec::new(),
            metadata: HashMap::new(),
        }
    }

    pub fn json(data: Vec<u8>) -> Self {
        let mut metadata = HashMap::new();
        metadata.insert("encoding".to_string(), b"json".to_vec());
        metadata.insert("content-type".to_string(), b"application/json".to_vec());
        Self { data, metadata }
    }

    pub fn binary(data: Vec<u8>) -> Self {
        let mut metadata = HashMap::new();
        metadata.insert("encoding".to_string(), b"binary".to_vec());
        metadata.insert(
            "content-type".to_string(),
            b"application/octet-stream".to_vec(),
        );
        Self { data, metadata }
    }

    pub fn protobuf(data: Vec<u8>) -> Self {
        let mut metadata = HashMap::new();
        metadata.insert("encoding".to_string(), b"binary/protobuf".to_vec());
        metadata.insert(
            "content-type".to_string(),
            b"application/x-protobuf".to_vec(),
        );
        Self { data, metadata }
    }

    /// Returns `None` if the key is missing or the value is not valid UTF-8.
    pub fn get_metadata_string(&self, key: &str) -> Option<String> {
        self.metadata
            .get(key)
            .and_then(|bytes| String::from_utf8(bytes.clone()).ok())
    }

    pub fn get_metadata_bytes(&self, key: &str) -> Option<&[u8]> {
        self.metadata.get(key).map(|v| v.as_slice())
    }

    pub fn set_metadata_string(&mut self, key: impl Into<String>, value: impl Into<String>) {
        self.metadata.insert(key.into(), value.into().into_bytes());
    }

    pub fn set_metadata_bytes(&mut self, key: impl Into<String>, value: Vec<u8>) {
        self.metadata.insert(key.into(), value);
    }

    pub fn remove_metadata(&mut self, key: impl Into<String>) -> Option<Vec<u8>> {
        self.metadata.remove(&key.into())
    }

    pub fn encoding(&self) -> Option<String> {
        self.get_metadata_string("encoding")
    }

    pub fn content_type(&self) -> Option<String> {
        self.get_metadata_string("content-type")
    }

    /// Matches any encoding containing "json" (e.g. "json/gzip", "json/gzip/encrypted").
    pub fn is_json(&self) -> bool {
        self.encoding()
            .map(|enc| enc.contains("json"))
            .unwrap_or(false)
    }

    /// Matches any encoding containing "binary" (e.g. "binary/protobuf").
    pub fn is_binary(&self) -> bool {
        self.encoding()
            .map(|enc| enc.contains("binary"))
            .unwrap_or(false)
    }

    /// Checks for "gzip" or "zstd" in the encoding string.
    pub fn is_compressed(&self) -> bool {
        self.encoding()
            .map(|enc| enc.contains("gzip") || enc.contains("zstd"))
            .unwrap_or(false)
    }

    /// Checks for "encrypted" in the encoding string.
    pub fn is_encrypted(&self) -> bool {
        self.encoding()
            .map(|enc| enc.contains("encrypted"))
            .unwrap_or(false)
    }

    pub fn size(&self) -> usize {
        self.data.len()
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    pub fn new_data(data: Vec<u8>) -> Self {
        Self {
            data,
            metadata: HashMap::new(),
        }
    }

    pub fn from_json<T: Serialize>(value: &T) -> Result<Self, OrcherError> {
        let data = serde_json::to_vec(value).map_err(|e| OrcherError::Serialization {
            format: "json".to_string(),
            message: e.to_string(),
        })?;
        Ok(Self::json(data))
    }

    pub fn to_json<T: for<'de> Deserialize<'de>>(&self) -> Result<T, OrcherError> {
        serde_json::from_slice(&self.data).map_err(|e| OrcherError::Serialization {
            format: "json".to_string(),
            message: e.to_string(),
        })
    }

    pub fn from_string(value: impl Into<String>) -> Self {
        let data = value.into().into_bytes();
        let mut metadata = HashMap::new();
        metadata.insert("encoding".to_string(), b"utf-8".to_vec());
        metadata.insert("content-type".to_string(), b"text/plain".to_vec());
        Self { data, metadata }
    }

    pub fn as_string(&self) -> Result<String, OrcherError> {
        String::from_utf8(self.data.clone()).map_err(|e| OrcherError::Serialization {
            format: "utf-8".to_string(),
            message: format!("Invalid UTF-8: {}", e),
        })
    }

    pub fn with_metadata(mut self, key: impl Into<String>, value: impl Into<Vec<u8>>) -> Self {
        self.metadata.insert(key.into(), value.into());
        self
    }

    /// Alias for `get_metadata_bytes`.
    pub fn get_metadata(&self, key: &str) -> Option<&[u8]> {
        self.get_metadata_bytes(key)
    }

    /// Alias for `size()`.
    pub fn len(&self) -> usize {
        self.data.len()
    }
}

impl Default for Payload {
    fn default() -> Self {
        Self::empty()
    }
}

/// Represents function arguments or return values as an ordered list of payloads.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Payloads {
    pub payloads: Vec<Payload>,
}

impl Payloads {
    pub fn new(payloads: Vec<Payload>) -> Self {
        Self { payloads }
    }

    pub fn empty() -> Self {
        Self {
            payloads: Vec::new(),
        }
    }

    pub fn from_single(payload: Payload) -> Self {
        Self {
            payloads: vec![payload],
        }
    }

    pub fn len(&self) -> usize {
        self.payloads.len()
    }

    pub fn is_empty(&self) -> bool {
        self.payloads.is_empty()
    }

    pub fn get(&self, index: usize) -> Option<&Payload> {
        self.payloads.get(index)
    }

    pub fn get_mut(&mut self, index: usize) -> Option<&mut Payload> {
        self.payloads.get_mut(index)
    }

    pub fn push(&mut self, payload: Payload) {
        self.payloads.push(payload);
    }

    pub fn iter(&self) -> impl Iterator<Item = &Payload> {
        self.payloads.iter()
    }

    pub fn iter_mut(&mut self) -> impl Iterator<Item = &mut Payload> {
        self.payloads.iter_mut()
    }
}

impl Default for Payloads {
    fn default() -> Self {
        Self::empty()
    }
}

impl From<Vec<Payload>> for Payloads {
    fn from(payloads: Vec<Payload>) -> Self {
        Self::new(payloads)
    }
}

impl From<Payload> for Payloads {
    fn from(payload: Payload) -> Self {
        Self::from_single(payload)
    }
}

impl IntoIterator for Payloads {
    type Item = Payload;
    type IntoIter = std::vec::IntoIter<Payload>;

    fn into_iter(self) -> Self::IntoIter {
        self.payloads.into_iter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_payload_json() {
        let data = br#"{"key":"value"}"#.to_vec();
        let payload = Payload::json(data.clone());

        assert_eq!(payload.data, data);
        assert_eq!(payload.encoding(), Some("json".to_string()));
        assert_eq!(payload.content_type(), Some("application/json".to_string()));
        assert!(payload.is_json());
        assert!(!payload.is_binary());
        assert!(!payload.is_compressed());
        assert!(!payload.is_encrypted());
    }

    #[test]
    fn test_payload_binary() {
        let data = vec![0x01, 0x02, 0x03];
        let payload = Payload::binary(data.clone());

        assert_eq!(payload.data, data);
        assert_eq!(payload.encoding(), Some("binary".to_string()));
        assert_eq!(
            payload.content_type(),
            Some("application/octet-stream".to_string())
        );
        assert!(!payload.is_json());
        assert!(payload.is_binary());
    }

    #[test]
    fn test_payload_protobuf() {
        let data = vec![0x08, 0x96, 0x01];
        let payload = Payload::protobuf(data.clone());

        assert_eq!(payload.data, data);
        assert_eq!(payload.encoding(), Some("binary/protobuf".to_string()));
        assert!(payload.is_binary());
    }

    #[test]
    fn test_payload_metadata() {
        let mut payload = Payload::json(vec![]);
        payload.set_metadata_string("custom-key", "custom-value");
        payload.set_metadata_bytes("binary-key", vec![0x01, 0x02]);

        assert_eq!(
            payload.get_metadata_string("custom-key"),
            Some("custom-value".to_string())
        );
        assert_eq!(
            payload.get_metadata_bytes("binary-key"),
            Some(&[0x01, 0x02][..])
        );
    }

    #[test]
    fn test_payload_compressed() {
        let mut payload = Payload::empty();
        payload.set_metadata_string("encoding", "json/gzip");

        assert!(payload.is_json());
        assert!(payload.is_compressed());
        assert!(!payload.is_encrypted());
    }

    #[test]
    fn test_payload_encrypted() {
        let mut payload = Payload::empty();
        payload.set_metadata_string("encoding", "json/gzip/encrypted");

        assert!(payload.is_json());
        assert!(payload.is_compressed());
        assert!(payload.is_encrypted());
    }

    #[test]
    fn test_payload_size() {
        let data = vec![1, 2, 3, 4, 5];
        let payload = Payload::json(data);

        assert_eq!(payload.size(), 5);
        assert!(!payload.is_empty());
    }

    #[test]
    fn test_payload_empty() {
        let payload = Payload::empty();

        assert!(payload.is_empty());
        assert_eq!(payload.size(), 0);
        assert!(payload.metadata.is_empty());
    }

    #[test]
    fn test_payloads_creation() {
        let payloads = Payloads::new(vec![
            Payload::json(br#"{"id":"123"}"#.to_vec()),
            Payload::json(br#"99.99"#.to_vec()),
        ]);

        assert_eq!(payloads.len(), 2);
        assert!(!payloads.is_empty());
    }

    #[test]
    fn test_payloads_empty() {
        let payloads = Payloads::empty();

        assert!(payloads.is_empty());
        assert_eq!(payloads.len(), 0);
    }

    #[test]
    fn test_payloads_from_single() {
        let payload = Payload::json(vec![]);
        let payloads = Payloads::from_single(payload);

        assert_eq!(payloads.len(), 1);
    }

    #[test]
    fn test_payloads_get() {
        let payloads = Payloads::new(vec![
            Payload::json(br#"first"#.to_vec()),
            Payload::json(br#"second"#.to_vec()),
        ]);

        assert!(payloads.get(0).is_some());
        assert!(payloads.get(1).is_some());
        assert!(payloads.get(2).is_none());
    }

    #[test]
    fn test_payloads_push() {
        let mut payloads = Payloads::empty();
        payloads.push(Payload::json(vec![]));
        payloads.push(Payload::binary(vec![]));

        assert_eq!(payloads.len(), 2);
    }

    #[test]
    fn test_payloads_iter() {
        let payloads = Payloads::new(vec![
            Payload::json(vec![]),
            Payload::binary(vec![]),
            Payload::json(vec![]),
        ]);

        let json_count = payloads.iter().filter(|p| p.is_json()).count();
        assert_eq!(json_count, 2);
    }

    #[test]
    fn test_payloads_from_vec() {
        let vec = vec![Payload::json(vec![]), Payload::binary(vec![])];
        let payloads: Payloads = vec.into();

        assert_eq!(payloads.len(), 2);
    }

    #[test]
    fn test_payloads_from_payload() {
        let payload = Payload::json(vec![]);
        let payloads: Payloads = payload.into();

        assert_eq!(payloads.len(), 1);
    }

    #[test]
    fn test_payloads_into_iter() {
        let payloads = Payloads::new(vec![Payload::json(vec![]), Payload::binary(vec![])]);

        let count = payloads.into_iter().count();
        assert_eq!(count, 2);
    }
}
