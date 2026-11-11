//! Integration utilities for automatic instrumentation of ORCHER components.
//!
//! This module provides seamless integration with other ORCHER crates, enabling
//! automatic instrumentation of common operations, storage queries, security events,
//! and component lifecycles.
//!
//! # Examples
//!
//! ```rust
//! use orcher_monitoring::integration::prelude::*;
//! ```

#[cfg(feature = "integration")]
pub mod common;

#[cfg(feature = "integration")]
pub mod storage;

#[cfg(feature = "integration")]
pub mod security;

pub mod scheduler;
pub mod worker;

/// Convenient prelude for integration utilities
pub mod prelude {
    #[cfg(feature = "integration")]
    pub use super::common::*;

    #[cfg(feature = "integration")]
    pub use super::storage::*;

    #[cfg(feature = "integration")]
    pub use super::security::*;

    pub use super::scheduler::*;
    pub use super::worker::*;
}
