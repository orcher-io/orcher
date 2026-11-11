//! Shared types, error handling, and utilities for the ORCHER platform.

#![warn(clippy::all)]
#![deny(unsafe_code)]

pub use jiff;
pub use serde;
pub use serde_json;

#[cfg(feature = "uuid")]
pub use uuid;

pub mod crypto;
pub mod error;
pub mod events;
pub mod payload;
pub mod prelude;
pub mod routing;
pub mod types;
pub mod version;

#[cfg(feature = "tokio")]
pub mod utils;

#[cfg(feature = "validation")]
pub mod validation;

#[cfg(feature = "monitoring")]
pub mod monitoring;

#[cfg(feature = "test-utils")]
pub mod testing;

pub use error::OrcherResult;

pub fn version() -> &'static str {
    version::VERSION
}

pub fn build_info() -> version::BuildInfo {
    version::build_info()
}

pub mod features {
    pub const fn has_tokio() -> bool {
        cfg!(feature = "tokio")
    }

    pub const fn has_uuid() -> bool {
        cfg!(feature = "uuid")
    }

    pub const fn has_monitoring() -> bool {
        cfg!(feature = "monitoring")
    }

    pub const fn has_validation() -> bool {
        cfg!(feature = "validation")
    }

    pub const fn has_test_utils() -> bool {
        cfg!(feature = "test-utils")
    }

    pub fn enabled_features() -> Vec<&'static str> {
        let mut features = Vec::new();

        if has_tokio() {
            features.push("tokio");
        }
        if has_uuid() {
            features.push("uuid");
        }
        if has_monitoring() {
            features.push("monitoring");
        }
        if has_validation() {
            features.push("validation");
        }
        if has_test_utils() {
            features.push("test-utils");
        }

        features
    }
}

// Compile-time feature checks
#[cfg(not(feature = "uuid"))]
compile_error!("The 'uuid' feature is required for basic ORCHER functionality. Enable it with: features = [\"uuid\"]");

// Optional warning for common development scenarios
#[cfg(all(debug_assertions, not(feature = "tokio")))]
compile_error!("The 'tokio' feature is highly recommended for async functionality. Enable it with: features = [\"tokio\"]");
