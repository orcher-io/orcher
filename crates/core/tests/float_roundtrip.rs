//! A task's result reaches its workflow with the numbers the task returned.
//!
//! The engine parses a task's result into a JSON value for its step row, and
//! replay reads that `jsonb` row back and serializes it into the workflow's
//! journal. Each hop parses floats; without serde_json's `float_roundtrip`,
//! about a third of `f64` values came back one unit in the last place off, so
//! a workflow could replay a different number from the one its task returned.

use serde_json::Value;

/// `count` f64 values spread over every exponent, from a fixed seed, so a
/// failure reproduces. Finite values only: JSON has no NaN or infinity.
fn floats(count: usize) -> Vec<f64> {
    let mut state: u64 = 0x9E37_79B9_7F4A_7C15;
    let mut out = Vec::with_capacity(count);
    while out.len() < count {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        let value = f64::from_bits(state);
        if value.is_finite() {
            out.push(value);
        }
    }
    out
}

/// The bits of every number in `value`, in order.
fn bits(value: &Value) -> Vec<u64> {
    value
        .as_array()
        .expect("an array of numbers")
        .iter()
        .map(|n| n.as_f64().expect("a number").to_bits())
        .collect()
}

#[test]
fn a_result_parsed_and_written_again_keeps_every_float() {
    let sent = Value::from(floats(100_000));
    let bytes = serde_json::to_vec(&sent).unwrap();
    let parsed: Value = serde_json::from_slice(&bytes).unwrap();
    let mismatched = bits(&sent)
        .iter()
        .zip(bits(&parsed))
        .filter(|(a, b)| **a != *b)
        .count();
    assert_eq!(
        mismatched, 0,
        "{mismatched} of 100000 floats changed when parsed"
    );
}

/// The step row's hop: a `jsonb` value written and read back through sqlx.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_result_stored_as_jsonb_and_read_back_keeps_every_float() {
    let url = std::env::var("TEST_DATABASE_URL").unwrap_or_else(|_| {
        "postgresql://postgres:postgres@localhost:5432/orcher_test".to_string()
    });
    let pool = sqlx::PgPool::connect(&url).await.expect("connect");
    let sent = Value::from(floats(20_000));
    let read: Value = sqlx::query_scalar("SELECT $1::jsonb")
        .bind(&sent)
        .fetch_one(&pool)
        .await
        .expect("round-trip through jsonb");
    let mismatched = bits(&sent)
        .iter()
        .zip(bits(&read))
        .filter(|(a, b)| **a != *b)
        .count();
    assert_eq!(
        mismatched, 0,
        "{mismatched} of 20000 floats changed in jsonb"
    );
}
