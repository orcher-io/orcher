//! Server-side Payload envelope detection and unwrapping.

use crate::error::{CoreError, CoreResult};
use base64::{engine::general_purpose::STANDARD as BASE64, Engine};
use serde_json::Value;
use tracing::{debug, trace, warn};

/// Decode input payload, handling both plain JSON and Payload envelope formats.
///
/// This is the main entry point for server-side payload processing. It:
/// 1. Detects if the input is a Payload envelope or plain JSON
/// 2. Unwraps the envelope if needed
/// 3. Returns the plain JSON data
///
/// # Arguments
///
/// * `input` - The input value (either plain JSON or Payload envelope)
///
/// # Returns
///
/// The unwrapped JSON value, ready for storage in the database.
///
/// # Errors
///
/// Returns an error if:
/// - The Payload envelope is malformed
/// - The data field cannot be decoded
/// - The encoding format is unsupported
///
/// # Examples
///
/// ```rust,ignore
/// use orcher_core::utils::payload_handler::decode_input_payload;
/// use serde_json::json;
///
/// // Plain JSON passes through
/// let plain = json!({"key": "value"});
/// let result = decode_input_payload(&plain)?;
/// assert_eq!(result, plain);
///
/// // Payload envelope is unwrapped
/// let envelope = json!({
///     "metadata": {"encoding": "json"},
///     "data": [123, 34, 107, 101, 121, 34, 58, 34, 118, 97, 108, 117, 101, 34, 125]
/// });
/// let result = decode_input_payload(&envelope)?;
/// assert_eq!(result, json!({"key": "value"}));
/// # Ok::<(), orcher_core::error::Error>(())
/// ```
pub fn decode_input_payload(input: &Value) -> CoreResult<Value> {
    // Check if this is a Payload envelope
    if is_payload_envelope(input) {
        debug!("Detected Payload envelope format, unwrapping...");
        unwrap_payload_envelope(input)
    } else {
        // Plain JSON - pass through
        trace!("Input is plain JSON, passing through");
        Ok(input.clone())
    }
}

/// Check if a JSON value is a Payload envelope.
///
/// A Payload envelope has the structure:
/// ```json
/// {
///   "metadata": { ... },
///   "data": [...]  // or "data": "base64string"
/// }
/// ```
///
/// # Arguments
///
/// * `value` - The JSON value to check
///
/// # Returns
///
/// `true` if the value appears to be a Payload envelope, `false` otherwise.
///
/// # Examples
///
/// ```rust
/// use orcher_core::utils::payload_handler::is_payload_envelope;
/// use serde_json::json;
///
/// // Payload envelope
/// let envelope = json!({
///     "metadata": {"encoding": "json"},
///     "data": [1, 2, 3]
/// });
/// assert!(is_payload_envelope(&envelope));
///
/// // Plain JSON
/// let plain = json!({"key": "value"});
/// assert!(!is_payload_envelope(&plain));
///
/// // Object with metadata but no data
/// let partial = json!({"metadata": {"encoding": "json"}});
/// assert!(!is_payload_envelope(&partial));
/// ```
pub fn is_payload_envelope(value: &Value) -> bool {
    if let Some(obj) = value.as_object() {
        // Must have both "metadata" and "data" fields
        let has_metadata = obj.contains_key("metadata");
        let has_data = obj.contains_key("data");

        if has_metadata && has_data {
            // Metadata should be an object
            let metadata_is_object = obj.get("metadata").map(|v| v.is_object()).unwrap_or(false);

            // Data should be an array (bytes) or string (base64)
            let data_is_valid = obj
                .get("data")
                .map(|v| v.is_array() || v.is_string())
                .unwrap_or(false);

            return metadata_is_object && data_is_valid;
        }
    }
    false
}

/// Unwrap a Payload envelope and extract the JSON data.
///
/// This function handles multiple encoding formats:
/// - JSON encoding with byte array data
/// - JSON encoding with base64 string data
/// - Binary/Protobuf encoding (returns data as-is)
///
/// # Arguments
///
/// * `envelope` - The Payload envelope value
///
/// # Returns
///
/// The unwrapped JSON data.
///
/// # Errors
///
/// Returns an error if:
/// - The envelope is malformed
/// - The encoding is unsupported
/// - The data cannot be decoded
///
/// # Examples
///
/// ```rust,ignore
/// use orcher_core::utils::payload_handler::unwrap_payload_envelope;
/// use serde_json::json;
///
/// let envelope = json!({
///     "metadata": {"encoding": "json"},
///     "data": [123, 34, 107, 101, 121, 34, 58, 34, 118, 97, 108, 117, 101, 34, 125]
/// });
/// let result = unwrap_payload_envelope(&envelope)?;
/// assert_eq!(result, json!({"key": "value"}));
/// # Ok::<(), orcher_core::error::Error>(())
/// ```
pub fn unwrap_payload_envelope(envelope: &Value) -> CoreResult<Value> {
    let obj = envelope.as_object().ok_or_else(|| {
        CoreError::ValidationFailed("Payload envelope must be an object".to_string())
    })?;

    // Extract metadata
    let metadata = obj
        .get("metadata")
        .and_then(|v| v.as_object())
        .ok_or_else(|| {
            CoreError::ValidationFailed("Payload envelope must have metadata object".to_string())
        })?;

    // Extract data
    let data = obj.get("data").ok_or_else(|| {
        CoreError::ValidationFailed("Payload envelope must have data field".to_string())
    })?;

    // Get encoding from metadata
    let encoding = metadata
        .get("encoding")
        .and_then(|v| v.as_str())
        .unwrap_or("json");

    debug!("Unwrapping payload with encoding: {}", encoding);

    // Handle different encoding types
    if encoding.contains("json") {
        decode_json_payload_data(data)
    } else if encoding.contains("protobuf") || encoding.contains("binary") {
        decode_binary_payload_data(data)
    } else {
        warn!("Unknown encoding '{}', attempting JSON decode", encoding);
        decode_json_payload_data(data)
    }
}

/// Decode JSON payload data from bytes or base64.
///
/// # Arguments
///
/// * `data` - The data field from the Payload envelope (array or string)
///
/// # Returns
///
/// The decoded JSON value.
///
/// # Errors
///
/// Returns an error if the data cannot be decoded as JSON.
fn decode_json_payload_data(data: &Value) -> CoreResult<Value> {
    let bytes = extract_bytes_from_data(data)?;

    // Parse the bytes as JSON
    serde_json::from_slice(&bytes)
        .map_err(|e| CoreError::Internal(format!("Failed to parse JSON from payload data: {}", e)))
}

/// Decode binary payload data (returns as-is for now).
///
/// In the future, this could handle Protobuf deserialization or other binary formats.
///
/// # Arguments
///
/// * `data` - The data field from the Payload envelope
///
/// # Returns
///
/// A JSON object with the raw bytes (for debugging/storage).
fn decode_binary_payload_data(data: &Value) -> CoreResult<Value> {
    let bytes = extract_bytes_from_data(data)?;

    // For binary data, we'll return a base64-encoded string wrapped in an object
    // This allows it to be stored in JSON format
    let base64_str = BASE64.encode(&bytes);
    Ok(serde_json::json!({
        "binary_data": base64_str,
        "length": bytes.len()
    }))
}

/// Extract bytes from the data field of a Payload.
///
/// The data field can be:
/// - An array of numbers (bytes)
/// - A base64-encoded string
///
/// # Arguments
///
/// * `data` - The data field value
///
/// # Returns
///
/// The extracted bytes.
///
/// # Errors
///
/// Returns an error if the data format is invalid.
fn extract_bytes_from_data(data: &Value) -> CoreResult<Vec<u8>> {
    match data {
        // Data as byte array: [123, 34, 107, 101, 121, ...]
        Value::Array(arr) => {
            let bytes: CoreResult<Vec<u8>> = arr
                .iter()
                .map(|v| {
                    v.as_u64()
                        .and_then(|n| u8::try_from(n).ok())
                        .ok_or_else(|| {
                            CoreError::ValidationFailed(format!("Invalid byte value: {}", v))
                        })
                })
                .collect();
            bytes
        }
        // Data as base64 string
        Value::String(s) => BASE64
            .decode(s)
            .map_err(|e| CoreError::Internal(format!("Failed to decode base64 data: {}", e))),
        _ => Err(CoreError::ValidationFailed(
            "Payload data must be an array or string".to_string(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_is_payload_envelope_with_valid_envelope() {
        let envelope = json!({
            "metadata": {"encoding": "json"},
            "data": [123, 34, 107, 101, 121, 34, 58, 34, 118, 97, 108, 117, 101, 34, 125]
        });
        assert!(is_payload_envelope(&envelope));
    }

    #[test]
    fn test_is_payload_envelope_with_base64_data() {
        let envelope = json!({
            "metadata": {"encoding": "json"},
            "data": "eyJrZXkiOiJ2YWx1ZSJ9"  // base64 encoded
        });
        assert!(is_payload_envelope(&envelope));
    }

    #[test]
    fn test_is_payload_envelope_with_plain_json() {
        let plain = json!({"key": "value"});
        assert!(!is_payload_envelope(&plain));
    }

    #[test]
    fn test_is_payload_envelope_missing_metadata() {
        let partial = json!({"data": [1, 2, 3]});
        assert!(!is_payload_envelope(&partial));
    }

    #[test]
    fn test_is_payload_envelope_missing_data() {
        let partial = json!({"metadata": {"encoding": "json"}});
        assert!(!is_payload_envelope(&partial));
    }

    #[test]
    fn test_is_payload_envelope_invalid_metadata_type() {
        let invalid = json!({
            "metadata": "not an object",
            "data": [1, 2, 3]
        });
        assert!(!is_payload_envelope(&invalid));
    }

    #[test]
    fn test_decode_input_payload_plain_json() {
        let plain = json!({"amount": 99.99, "currency": "USD"});
        let result = decode_input_payload(&plain).unwrap();
        assert_eq!(result, plain);
    }

    #[test]
    fn test_decode_input_payload_with_byte_array() {
        // {"key":"value"} as bytes
        let bytes = vec![
            123, 34, 107, 101, 121, 34, 58, 34, 118, 97, 108, 117, 101, 34, 125,
        ];
        let envelope = json!({
            "metadata": {"encoding": "json", "content-type": "application/json"},
            "data": bytes
        });

        let result = decode_input_payload(&envelope).unwrap();
        assert_eq!(result, json!({"key": "value"}));
    }

    #[test]
    fn test_decode_input_payload_with_base64() {
        // {"key":"value"} as base64
        let base64_data = "eyJrZXkiOiJ2YWx1ZSJ9";
        let envelope = json!({
            "metadata": {"encoding": "json"},
            "data": base64_data
        });

        let result = decode_input_payload(&envelope).unwrap();
        assert_eq!(result, json!({"key": "value"}));
    }

    #[test]
    fn test_decode_input_payload_complex_object() {
        // Complex object: {"order_id": "123", "items": ["a", "b"], "total": 99.99}
        let json_str = r#"{"order_id":"123","items":["a","b"],"total":99.99}"#;
        let bytes: Vec<u8> = json_str.as_bytes().to_vec();

        let envelope = json!({
            "metadata": {"encoding": "json"},
            "data": bytes
        });

        let result = decode_input_payload(&envelope).unwrap();
        let expected = json!({
            "order_id": "123",
            "items": ["a", "b"],
            "total": 99.99
        });
        assert_eq!(result, expected);
    }

    #[test]
    fn test_unwrap_payload_envelope_missing_metadata() {
        let invalid = json!({"data": [1, 2, 3]});
        let result = unwrap_payload_envelope(&invalid);
        assert!(result.is_err());
    }

    #[test]
    fn test_unwrap_payload_envelope_missing_data() {
        let invalid = json!({"metadata": {"encoding": "json"}});
        let result = unwrap_payload_envelope(&invalid);
        assert!(result.is_err());
    }

    #[test]
    fn test_extract_bytes_from_byte_array() {
        let data = json!([72, 101, 108, 108, 111]); // "Hello"
        let bytes = extract_bytes_from_data(&data).unwrap();
        assert_eq!(bytes, b"Hello");
    }

    #[test]
    fn test_extract_bytes_from_base64() {
        let data = json!("SGVsbG8="); // "Hello" in base64
        let bytes = extract_bytes_from_data(&data).unwrap();
        assert_eq!(bytes, b"Hello");
    }

    #[test]
    fn test_extract_bytes_invalid_type() {
        let data = json!(123);
        let result = extract_bytes_from_data(&data);
        assert!(result.is_err());
    }

    #[test]
    fn test_extract_bytes_invalid_byte_value() {
        let data = json!([256]); // Out of u8 range
        let result = extract_bytes_from_data(&data);
        assert!(result.is_err());
    }

    #[test]
    fn test_decode_json_payload_data() {
        let bytes = br#"{"test":true}"#.to_vec();
        let data = json!(bytes);
        let result = decode_json_payload_data(&data).unwrap();
        assert_eq!(result, json!({"test": true}));
    }

    #[test]
    fn test_decode_binary_payload_data() {
        let bytes = vec![1, 2, 3, 4, 5];
        let data = json!(bytes);
        let result = decode_binary_payload_data(&data).unwrap();

        // Should return base64-encoded data
        assert!(result.get("binary_data").is_some());
        assert_eq!(result.get("length"), Some(&json!(5)));
    }

    #[test]
    fn test_typescript_sdk_style_envelope() {
        // This is what the TypeScript SDK sends
        let typescript_envelope = json!({
            "metadata": {
                "encoding": "json/plain",
                "messageType": "order"
            },
            "data": [123, 34, 97, 109, 111, 117, 110, 116, 34, 58, 57, 57, 46, 57, 57, 125]
        });

        let result = decode_input_payload(&typescript_envelope).unwrap();
        assert_eq!(result, json!({"amount": 99.99}));
    }

    #[test]
    fn test_nested_json_object() {
        let json_str = r#"{"user":{"id":1,"name":"Alice"},"settings":{"theme":"dark"}}"#;
        let bytes: Vec<u8> = json_str.as_bytes().to_vec();

        let envelope = json!({
            "metadata": {"encoding": "json"},
            "data": bytes
        });

        let result = decode_input_payload(&envelope).unwrap();
        assert_eq!(
            result,
            json!({
                "user": {"id": 1, "name": "Alice"},
                "settings": {"theme": "dark"}
            })
        );
    }

    #[test]
    fn test_unicode_content() {
        let json_str = r#"{"message":"Hello 世界 🌍"}"#;
        let bytes: Vec<u8> = json_str.as_bytes().to_vec();

        let envelope = json!({
            "metadata": {"encoding": "json"},
            "data": bytes
        });

        let result = decode_input_payload(&envelope).unwrap();
        assert_eq!(result, json!({"message": "Hello 世界 🌍"}));
    }
}
