//! # Mock Implementations for Testing
//!
//! This module provides mock implementations of storage components
//! for testing purposes, integrating with orcher-common types.

pub mod repositories;

// Re-export commonly used mocks
pub use repositories::*;
