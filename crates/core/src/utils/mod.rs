//! Utility functions and helpers

/// ID generation utilities
pub mod id {
    /// Generate a new unique identifier as a string
    pub fn generate() -> String {
        uuid::Uuid::new_v4().to_string()
    }
}

/// Time utilities
pub mod time {
    use orcher_common::types::time::Timestamptz;

    /// Get the current UTC timestamp
    pub fn now() -> Timestamptz {
        Timestamptz::now()
    }
}

/// Payload handling utilities for detecting and unwrapping Payload envelopes
pub mod payload_handler;
