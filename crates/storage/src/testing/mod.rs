//! # Storage Testing Utilities and Framework
//!
//! This module provides comprehensive testing utilities and framework components
//! for the ORCHER storage layer. It integrates with orcher-common test utilities
//! and provides storage-specific testing patterns.
//!
//! ## Features
//!
//! - Integration with orcher-common test utilities
//! - Storage-specific mock implementations
//! - Test fixtures for storage entities
//! - Performance testing utilities
//! - Integration test helpers

#[cfg(feature = "test-utils")]
pub mod integration;

#[cfg(feature = "test-utils")]
pub mod mocks;

#[cfg(feature = "test-utils")]
pub mod fixtures;

#[cfg(feature = "test-utils")]
pub mod performance;

#[cfg(feature = "test-utils")]
pub mod assertions;

#[cfg(feature = "test-utils")]
pub mod generators;

#[cfg(feature = "test-utils")]
pub mod containers;

#[cfg(feature = "test-utils")]
pub mod scenarios;

// Re-export orcher-common testing utilities for convenience
#[cfg(feature = "test-utils")]
pub use orcher_common::testing::*;
