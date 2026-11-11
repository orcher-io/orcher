//! # Payload Envelope Integration Tests
//!
//! Integration tests for server-side payload envelope detection and unwrapping.
//!
//! These tests verify that the payload_handler module correctly processes both:
//! 1. Plain JSON inputs (backward compatibility)
//! 2. Payload envelope inputs (TypeScript SDK format)
//!
//! ## Test Coverage
//!
//! 1. Plain JSON detection and pass-through
//! 2. Payload envelope detection and unwrapping
//! 3. TypeScript SDK style envelope compatibility
//! 4. Complex nested objects with Unicode
//! 5. Base64-encoded data decoding

use orcher_core::utils::payload_handler::{decode_input_payload, is_payload_envelope};
use serde_json::json;

#[test]
fn test_plain_json_pass_through() {
    // Plain JSON should pass through untouched
    let plain = json!({
        "orderId": "ORD-12345",
        "amount": 99.99,
        "currency": "USD"
    });

    let result = decode_input_payload(&plain).expect("Should decode plain JSON");

    assert_eq!(result, plain);
    assert_eq!(result["orderId"], "ORD-12345");
    assert_eq!(result["amount"], 99.99);
    assert_eq!(result["currency"], "USD");

    println!("✅ Plain JSON passed through correctly");
}

#[test]
fn test_payload_envelope_detection() {
    // Plain JSON should not be detected as envelope
    let plain = json!({"key": "value"});
    assert!(!is_payload_envelope(&plain));

    // Payload envelope should be detected
    let envelope = json!({
        "metadata": {"encoding": "json"},
        "data": [123, 34, 107, 101, 121, 34, 58, 34, 118, 97, 108, 117, 101, 34, 125]
    });
    assert!(is_payload_envelope(&envelope));

    println!("✅ Envelope detection working correctly");
}

#[test]
fn test_payload_envelope_unwrapping() {
    // Payload envelope with byte array data
    let envelope = json!({
        "metadata": {
            "encoding": "json",
            "content-type": "application/json"
        },
        "data": [123, 34, 111, 114, 100, 101, 114, 73, 100, 34, 58, 34, 79, 82, 68, 45, 54, 55, 56, 57, 48, 34, 44, 34, 97, 109, 111, 117, 110, 116, 34, 58, 49, 50, 51, 46, 52, 53, 125]
        // {"orderId":"ORD-67890","amount":123.45}
    });

    let result = decode_input_payload(&envelope).expect("Should unwrap envelope");

    // Should extract the actual JSON, not the envelope
    assert_eq!(result["orderId"], "ORD-67890");
    assert_eq!(result["amount"], 123.45);

    // Should NOT have envelope fields
    assert!(result.get("metadata").is_none());
    assert!(result.get("data").is_none());

    println!("✅ Payload envelope unwrapped correctly");
}

#[test]
fn test_typescript_sdk_envelope() {
    // TypeScript SDK style with additional metadata
    let typescript_envelope = json!({
        "metadata": {
            "encoding": "json/plain",
            "messageType": "OrderRequest",
            "content-type": "application/json"
        },
        "data": [123, 34, 99, 117, 115, 116, 111, 109, 101, 114, 73, 100, 34, 58, 34, 67, 85, 83, 84, 45, 49, 50, 51, 34, 44, 34, 105, 116, 101, 109, 115, 34, 58, 91, 34, 119, 105, 100, 103, 101, 116, 34, 44, 34, 103, 97, 100, 103, 101, 116, 34, 93, 125]
        // {"customerId":"CUST-123","items":["widget","gadget"]}
    });

    let result =
        decode_input_payload(&typescript_envelope).expect("Should handle TypeScript SDK envelope");

    assert_eq!(result["customerId"], "CUST-123");
    assert_eq!(result["items"][0], "widget");
    assert_eq!(result["items"][1], "gadget");

    println!("✅ TypeScript SDK envelope handled correctly");
}

#[test]
fn test_base64_encoded_envelope() {
    // Payload envelope with base64-encoded data
    let base64_envelope = json!({
        "metadata": {
            "encoding": "json",
            "content-type": "application/json"
        },
        "data": "eyJ1c2VySWQiOiJVU0VSLTk5OSIsImFjdGlvbiI6ImxvZ2luIn0="
        // base64 for: {"userId":"USER-999","action":"login"}
    });

    let result = decode_input_payload(&base64_envelope).expect("Should decode base64 envelope");

    assert_eq!(result["userId"], "USER-999");
    assert_eq!(result["action"], "login");

    println!("✅ Base64-encoded envelope decoded correctly");
}

#[test]
fn test_complex_nested_object() {
    // Complex nested object with Unicode
    let complex_input = json!({
        "order": {
            "id": "ORD-COMPLEX-1",
            "customer": {
                "id": "CUST-456",
                "name": "Alice Müller",
                "email": "alice@example.com"
            },
            "items": [
                {
                    "sku": "WIDGET-001",
                    "quantity": 2,
                    "price": 19.99
                },
                {
                    "sku": "GADGET-002",
                    "quantity": 1,
                    "price": 49.99
                }
            ],
            "total": 89.97,
            "currency": "EUR",
            "metadata": {
                "source": "web",
                "campaign": "春季セール" // Japanese text
            }
        }
    });

    let result = decode_input_payload(&complex_input).expect("Should handle complex object");

    // Verify complex nested structure is preserved
    assert_eq!(result["order"]["id"], "ORD-COMPLEX-1");
    assert_eq!(result["order"]["customer"]["name"], "Alice Müller");
    assert_eq!(result["order"]["items"][0]["sku"], "WIDGET-001");
    assert_eq!(result["order"]["items"][1]["price"], 49.99);
    assert_eq!(result["order"]["total"], 89.97);

    // Verify Unicode is preserved
    assert_eq!(result["order"]["metadata"]["campaign"], "春季セール");

    println!("✅ Complex nested object with Unicode handled correctly");
}

#[test]
fn test_envelope_with_complex_nested_data() {
    // Create complex data as bytes
    let json_str = r#"{"user":{"id":1,"name":"Bob"},"settings":{"theme":"dark","language":"en"}}"#;
    let bytes: Vec<u8> = json_str.as_bytes().to_vec();
    let byte_values: Vec<serde_json::Value> = bytes.iter().map(|b| json!(*b)).collect();

    let envelope = json!({
        "metadata": {"encoding": "json"},
        "data": byte_values
    });

    let result = decode_input_payload(&envelope).expect("Should unwrap complex envelope");

    assert_eq!(result["user"]["id"], 1);
    assert_eq!(result["user"]["name"], "Bob");
    assert_eq!(result["settings"]["theme"], "dark");
    assert_eq!(result["settings"]["language"], "en");

    println!("✅ Envelope with complex nested data unwrapped correctly");
}

#[test]
fn test_backward_compatibility_all_formats() {
    // Test that various plain JSON formats work
    let test_cases = vec![
        json!({"simple": "value"}),
        json!({"number": 42}),
        json!({"array": [1, 2, 3]}),
        json!({"nested": {"deep": {"value": true}}}),
        json!(null),
        json!([1, 2, 3]),
        json!("plain string"),
        json!(123),
        json!(true),
    ];

    for (i, test_case) in test_cases.iter().enumerate() {
        let result = decode_input_payload(test_case)
            .unwrap_or_else(|_| panic!("Test case {} should succeed", i));
        assert_eq!(result, *test_case, "Test case {} should pass through", i);
    }

    println!("✅ All plain JSON formats maintain backward compatibility");
}

#[test]
fn test_roundtrip_consistency() {
    // Data should be identical after encode (by SDK) -> decode (by server)
    let original_data = json!({
        "workflowId": "WF-123",
        "status": "running",
        "progress": 75.5
    });

    // Simulate SDK encoding it as envelope
    let bytes: Vec<u8> = serde_json::to_vec(&original_data).unwrap();
    let byte_values: Vec<serde_json::Value> = bytes.iter().map(|b| json!(*b)).collect();

    let envelope = json!({
        "metadata": {"encoding": "json", "content-type": "application/json"},
        "data": byte_values
    });

    // Server decodes it
    let decoded = decode_input_payload(&envelope).expect("Should decode");

    // Should match original
    assert_eq!(decoded, original_data);

    println!("✅ Roundtrip consistency verified");
}

#[test]
fn test_empty_and_null_handling() {
    // Empty object
    let empty_obj = json!({});
    let result = decode_input_payload(&empty_obj).expect("Should handle empty object");
    assert_eq!(result, empty_obj);

    // Null value
    let null_val = json!(null);
    let result = decode_input_payload(&null_val).expect("Should handle null");
    assert_eq!(result, null_val);

    println!("✅ Empty and null values handled correctly");
}

#[test]
fn test_malformed_envelope_handling() {
    // Missing metadata
    let bad_envelope1 = json!({
        "data": [1, 2, 3]
    });
    assert!(!is_payload_envelope(&bad_envelope1));
    // Should pass through as plain JSON
    let result = decode_input_payload(&bad_envelope1).expect("Should pass through");
    assert_eq!(result, bad_envelope1);

    // Missing data
    let bad_envelope2 = json!({
        "metadata": {"encoding": "json"}
    });
    assert!(!is_payload_envelope(&bad_envelope2));
    let result = decode_input_payload(&bad_envelope2).expect("Should pass through");
    assert_eq!(result, bad_envelope2);

    // Invalid metadata type
    let bad_envelope3 = json!({
        "metadata": "not an object",
        "data": [1, 2, 3]
    });
    assert!(!is_payload_envelope(&bad_envelope3));

    println!("✅ Malformed envelopes handled gracefully");
}
